"""
DAG: Upcoming Schedule Checker
Author: jsangmin <jsm20up@gmail.com>
Description: 
    현재 활성화(Unpaused)된 모든 DAG의 예정된 실행 스케줄(Next Run) 정보를 조회합니다.
    앞으로 실행될 DAG 목록과 예정 시간을 확인하여 스케줄링 현황을 파악할 수 있습니다.
"""
from datetime import datetime, timedelta
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel
from airflow.utils.session import provide_session

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger("airflow_upcoming_schedule")

@provide_session
def check_upcoming_schedules(session=None):
    """
    활성화된 DAG들의 다음 실행 예정 시간(Next Run)을 조회합니다.
    """
    # Active 상태이고 Pause 되지 않은 DAG 조회
    dags = session.query(DagModel).filter(
        DagModel.is_active == True,
        DagModel.is_paused == False
    ).all()

    upcoming_list = []

    for dag in dags:
        # next_dagrun은 실제 run의 execution_date(logical date)를 의미합니다.
        # next_dagrun_create_after는 실제로 스케줄러가 DagRun을 생성할 수 있는 시간(Wall clock time)입니다.
        
        # 스케줄이 없는 경우(None) 제외
        if not dag.next_dagrun:
            continue

        dag_info = {
            'dag_id': dag.dag_id,
            'is_paused': dag.is_paused,
            'is_active': dag.is_active,
            'is_subdag': dag.is_subdag,
            'fileloc': dag.fileloc,
            'owners': dag.owners,
            'description': dag.description,
            'default_view': dag.default_view,
            'tags': [tag.name for tag in dag.tags] if hasattr(dag, 'tags') else [],
            'schedule_interval': str(dag.schedule_interval),
            'timetable_description': dag.timetable_summary if hasattr(dag, 'timetable_summary') else None,
            'next_run_logical_date': dag.next_dagrun.isoformat() if dag.next_dagrun else None,
            'next_run_create_after': dag.next_dagrun_create_after.isoformat() if dag.next_dagrun_create_after else None,
            'last_parsed_time': dag.last_parsed_time.isoformat() if dag.last_parsed_time else None,
            'last_pickled': dag.last_pickled.isoformat() if dag.last_pickled else None,
            'last_expired': dag.last_expired.isoformat() if dag.last_expired else None,
            'scheduler_lock': dag.scheduler_lock,
            'pickle_id': dag.pickle_id,
            'root_dag_id': dag.root_dag_id,
            'concurrency': dag.concurrency, # Deprecated in newer versions but still on model alias for max_active_tasks
            'max_active_tasks': dag.max_active_tasks,
            'max_active_runs': dag.max_active_runs,
            'has_task_concurrency_limits': dag.has_task_concurrency_limits,
            'has_import_errors': dag.has_import_errors,
        }
        upcoming_list.append(dag_info)

    # 실행 예정 시간이 빠른 순서대로 정렬 (Wall Clock Time 기준)
    upcoming_list.sort(key=lambda x: x['next_run_create_after'] if x['next_run_create_after'] else "9999-12-31")

    # 결과 로깅
    logger.info(f"Found {len(upcoming_list)} scheduled DAGs.")
    logger.info(json.dumps(upcoming_list, indent=2, ensure_ascii=False))

    return upcoming_list

with DAG(
    'upcoming_schedule_checker',
    default_args=default_args,
    description='List upcoming scheduled DAG runs',
    schedule_interval='0 7 * * *', # 매일 아침 07:00 (예시)
    catchup=False,
    tags=['monitoring', 'planning'],
) as dag:

    checker = PythonOperator(
        task_id='check_upcoming_schedules',
        python_callable=check_upcoming_schedules
    )
