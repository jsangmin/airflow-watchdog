"""
DAG: Daily DAG History Collector
Author: jsangmin <jsm20up@gmail.com>
Description: 
    오늘 실행된 모든 DAG와 Task의 상세 이력(실행 시간, 상태, Retry 횟수, 로그 URL 등)을 
    수집하여 로그로 출력하거나 추후 분석용으로 활용하기 위한 DAG입니다.
    
    수집 항목:
    - DAG Run: ID, 상태, 시작/종료 시간, 총 소요 시간
    - Task Instance: ID, 상태, 재시도 횟수, 실행 워커(Hostname), 로그 URL
"""
from datetime import datetime, timedelta, time
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.session import provide_session
from sqlalchemy import and_

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger("airflow_daily_collector")

@provide_session
def collect_daily_dag_history(session=None):
    """
    오늘 실행된 모든 DAG Run과 그에 속한 TaskInstance의 상세 정보를 수집합니다.
    """
    # 오늘 날짜 범위 설정 (UTC 기준)
    now = timezone.utcnow()
    today_start = timezone.make_aware(datetime.combine(now.date(), time.min))
    today_end = timezone.make_aware(datetime.combine(now.date(), time.max))
    
    logger.info(f"Collecting DAG history for date range: {today_start} ~ {today_end} (UTC)")

    # 1. 오늘 실행 이력이 있는 DagRun 조회
    dag_runs = session.query(DagRun).filter(
        DagRun.start_date >= today_start,
        DagRun.start_date <= today_end
    ).all()

    # 1-1. 해당 기간의 모든 TaskInstance를 한 번에 조회 (N+1 문제 해결)
    task_instances = session.query(TaskInstance).filter(
        TaskInstance.start_date >= today_start,
        TaskInstance.start_date <= today_end
    ).all()

    # TaskInstance를 (dag_id, run_id)를 키로 하여 그룹핑
    from collections import defaultdict
    ti_map = defaultdict(list)
    for ti in task_instances:
        ti_map[(ti.dag_id, ti.run_id)].append(ti)

    report_data = []

    for dr in dag_runs:
        dag_id = dr.dag_id
        
        # DAG Run 기본 정보
        run_info = {
            'dag_id': dag_id,
            'run_id': dr.run_id,
            'state': dr.state,
            'start_date': dr.start_date.isoformat() if dr.start_date else None,
            'end_date': dr.end_date.isoformat() if dr.end_date else None,
            'duration': (dr.end_date - dr.start_date).total_seconds() if dr.end_date and dr.start_date else None,
            'tasks': []
        }

        # 메모리에 로드된 TaskInstance 매핑 정보 사용
        tis = ti_map.get((dr.dag_id, dr.run_id), [])

        for ti in tis:
            # Task 기본 정보 및 로그 정보
            # try_number는 현재 시도 횟수이므로, 이미 실행된 횟수는 try_number - 1 (상태에 따라 다를 수 있음)
            # 로그 URL은 Airflow UI에서 접근 가능한 링크입니다.
            # 물리적 로그 파일 경로는 hostname과 log_id 조합으로 구성되지만, 설정에 따라 다르므로 log_url을 주로 사용합니다.
            
            task_info = {
                'task_id': ti.task_id,
                'state': ti.state,
                'try_number': ti.try_number, 
                'retry_count': max(0, ti.try_number - 1), # 재시도 횟수 추정
                'start_date': ti.start_date.isoformat() if ti.start_date else None,
                'end_date': ti.end_date.isoformat() if ti.end_date else None,
                'duration': ti.duration, # float (seconds)
                'log_url': ti.log_url,
                'hostname': ti.hostname, # 로그 파일이 위치한 워커 호스트
                'map_index': ti.map_index if hasattr(ti, 'map_index') else -1 # Dynamic Task Mapping 고려
            }
            run_info['tasks'].append(task_info)
        
        report_data.append(run_info)

    # 2. 결과 처리 (여기서는 로그로 출력하고 JSON 덤프)
    # 실제 환경에서는 이 데이터를 S3에 저장하거나 DB에 적재할 수 있습니다.
    report_json = json.dumps(report_data, indent=2, ensure_ascii=False)
    
    logger.info("--- Daily DAG History Report ---")
    logger.info(report_json)
    logger.info("--------------------------------")
    
    return report_data

with DAG(
    'daily_dag_history_collector',
    default_args=default_args,
    description='Collect statistics and log info for DAGs executed today',
    schedule_interval='50 23 * * *', # 매일 23:50에 실행하여 하루치 수집 (UTC 기준 유의)
    catchup=False,
    tags=['monitoring', 'reporting'],
) as dag:

    collector = PythonOperator(
        task_id='collect_daily_dag_history',
        python_callable=collect_daily_dag_history
    )
