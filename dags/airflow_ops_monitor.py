"""
DAG: Airflow Ops Monitor
Author: jsangmin <jsm20up@gmail.com>
Description: 
    Airflow 운영 안정성을 모니터링하기 위한 시스템 DAG입니다.
    주기적으로 다음 항목들을 점검합니다:
    1. Import Errors: 문법 오류 등으로 로드되지 않는 DAG 감지
    2. Stuck/Long Running Tasks: 멈춰있거나 비정상적으로 오래 걸리는 태스크 감지
    3. Failed DAGs: 최근 발생한 DAG 실패 이력 감지
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag, DagRun, TaskInstance, DagModel
from airflow.utils.state import State
from airflow.utils.session import provide_session
from sqlalchemy import func

# 설정: 임계값 정의
LONG_RUNNING_THRESHOLD = timedelta(hours=1)  # 1시간 이상 실행 중인 태스크
STUCK_QUEUED_THRESHOLD = timedelta(minutes=30)  # 30분 이상 Queued 상태인 태스크

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger("airflow_monitor")

@provide_session
def check_import_errors(session=None):
    """
    DB(ImportError 모델)를 조회하여 Import Error가 있는 DAG을 감지합니다.
    DagBag()을 사용하면 모든 파이썬 파일을 파싱하므로 부하가 큽니다.
    """
    from airflow.models import ImportError
    
    errors = session.query(ImportError).all()
    
    if errors:
        error_count = len(errors)
        error_msg = f"Found {error_count} DAG import errors:\n"
        for err in errors:
            error_msg += f"File: {err.filename}\nError: {err.stacktrace}\n{'-'*50}\n"
        
        logger.error(error_msg)
    else:
        logger.info("No DAG import errors found.")

@provide_session
def check_stuck_tasks(session=None):
    """
    오랫동안 Queued 상태이거나 Running 상태인 Task를 감지합니다.
    """
    now = datetime.now()
    
    # 1. Long Running Tasks
    long_running_tis = session.query(TaskInstance).filter(
        TaskInstance.state == State.RUNNING,
        TaskInstance.start_date < (now - LONG_RUNNING_THRESHOLD)
    ).all()

    if long_running_tis:
        msg = f"Found {len(long_running_tis)} tasks running longer than {LONG_RUNNING_THRESHOLD}:\n"
        for ti in long_running_tis:
            msg += f"DAG: {ti.dag_id}, Task: {ti.task_id}, Started: {ti.start_date}\n"
        logger.warning(msg)
    else:
        logger.info("No long-running tasks found.")
        
    # 2. Stuck Queued Tasks
    # queued_dttm이 존재하고 일정 시간이 지났는데도 여전히 Queued인 경우
    stuck_queued_tis = session.query(TaskInstance).filter(
        TaskInstance.state == State.QUEUED,
        TaskInstance.queued_dttm < (now - STUCK_QUEUED_THRESHOLD)
    ).all()
    
    if stuck_queued_tis:
        msg = f"Found {len(stuck_queued_tis)} tasks stuck in queued state longer than {STUCK_QUEUED_THRESHOLD}:\n"
        for ti in stuck_queued_tis:
            msg += f"DAG: {ti.dag_id}, Task: {ti.task_id}, Queued At: {ti.queued_dttm}\n"
        logger.warning(msg)
    else:
        logger.info("No stuck queued tasks found.")

@provide_session
def check_dag_failures(session=None):
    """
    최근 24시간 내 실패한 DAG Run을 확인합니다.
    """
    check_since = datetime.now() - timedelta(days=1)
    
    failed_runs = session.query(DagRun).filter(
        DagRun.state == State.FAILED,
        DagRun.execution_date >= check_since
    ).all()
    
    if failed_runs:
        msg = f"Found {len(failed_runs)} failed DAG runs in the last 24 hours:\n"
        for dr in failed_runs:
            msg += f"DAG: {dr.dag_id}, RunID: {dr.run_id}, Executed: {dr.execution_date}\n"
        logger.error(msg)
    else:
        logger.info("No failed DAG runs found in the last 24 hours.")

with DAG(
    'airflow_ops_monitor',
    default_args=default_args,
    description='Monitor Airflow DAGs for errors, stuck tasks, and failures',
    schedule_interval='@hourly', # 1시간마다 실행
    catchup=False,
    tags=['monitoring', 'ops'],
) as dag:

    t1 = PythonOperator(
        task_id='check_import_errors',
        python_callable=check_import_errors
    )

    t2 = PythonOperator(
        task_id='check_stuck_tasks',
        python_callable=check_stuck_tasks
    )

    t3 = PythonOperator(
        task_id='check_dag_failures',
        python_callable=check_dag_failures
    )

    # 병렬 실행
    [t1, t2, t3]
