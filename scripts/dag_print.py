from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag, DagRun
from airflow.utils.session import provide_session
from datetime import datetime
import os

# =================================================================
# [설정] 여기에 확인하고 싶은 DAG의 ID를 입력하세요
# =================================================================
TARGET_DAG_ID = "찾고_싶은_DAG_ID_를_여기에_입력" 
# 예: TARGET_DAG_ID = "tutorial"
# =================================================================

@provide_session
def print_specific_dag_code(session=None):
    print(f"🎯 분석 대상 DAG ID: {TARGET_DAG_ID}")
    
    # -------------------------------------------------------
    # 1. DagBag을 통해 DAG 객체 및 파일 경로 찾기
    # -------------------------------------------------------
    print("📦 DagBag 로드 중... (전체 DAG 스캔)")
    dagbag = DagBag()
    
    if TARGET_DAG_ID not in dagbag.dags:
        print(f"❌ 에러: DagBag에서 '{TARGET_DAG_ID}'를 찾을 수 없습니다.")
        print(f"   (힌트: DAG ID 스펠링이 정확한지, 또는 DAG 파일에 문법 오류가 없는지 확인하세요.)")
        return

    # DAG 객체 가져오기
    target_dag = dagbag.get_dag(TARGET_DAG_ID)
    file_path = target_dag.fileloc
    
    print(f"✅ DAG를 찾았습니다!")
    print(f"📂 파일 경로: {file_path}")

    # -------------------------------------------------------
    # 2. DagRun을 통해 최근 실행 상태 확인 (옵션)
    # -------------------------------------------------------
    # 코드를 보기 전에, 이 DAG가 최근에 어떻게 돌았는지 확인
    last_run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == TARGET_DAG_ID)
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    
    if last_run:
        print(f"📊 최근 실행 상태: {last_run.state} (실행일: {last_run.execution_date})")
    else:
        print(f"📊 최근 실행 기록이 없습니다.")

    # -------------------------------------------------------
    # 3. 소스 코드 읽기 및 출력
    # -------------------------------------------------------
    try:
        if os.path.exists(file_path):
            print(f"\n{'='*25} [ SOURCE CODE START ] {'='*25}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                code_content = f.read()
                print(code_content)
                
            print(f"{'='*26} [ SOURCE CODE END ] {'='*26}\n")
        else:
            print("❌ 파일 경로에 실제 파일이 존재하지 않습니다. (삭제됨 혹은 워커 노드 경로 상이)")
            
    except Exception as e:
        print(f"❌ 파일 읽기 실패: {str(e)}")


# -------------------------------------------------------
# 리포트 실행을 위한 DAG 정의
# -------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='print_specific_dag_code',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['debug', 'manual_check'],
) as dag:

    # Task 정의
    check_code_task = PythonOperator(
        task_id='print_code',
        python_callable=print_specific_dag_code,
    )