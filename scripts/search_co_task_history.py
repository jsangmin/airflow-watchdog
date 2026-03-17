#!/usr/bin/env python3
"""
Airflow Task Execution History Search
Author: jsangmin <jsm20up@gmail.com>

Description:
    This script queries the execution history of tasks for DAGs owned by 'CO'
    that have been executed after a given last_search_date.
    It returns/prints the most recent execution start date, which can be used as
    the last_search_date for the next run.

    이 스크립트는 owner가 'CO'인 DAG 중 주어진 last_search_date 이후에 실행된 Task 시작시간, 종료시간, 소요시간, 성공/실패 상태 및 코드 내용을 조회합니다.
    마지막으로 가장 최근 실행된 시간을 찾아 [NEXT_SEARCH_DATE] 로 출력 및 반환하며, 이는 다음 조회 시 파라미터로 사용할 수 있습니다.
    
Usage:
    python search_co_task_history.py --last-search-date "YYYY-MM-DD HH:MM:SS"
    Example: python search_co_task_history.py --last-search-date "2023-10-01"
"""

import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd

from airflow.models import DagModel, TaskInstance, DagRun
from airflow.models.dagbag import DagBag
from airflow.utils.session import provide_session
from airflow.utils import timezone


@provide_session
def search_task_history(last_search_date_str, owner_pattern="CO", session=None):
    try:
        last_search_date = timezone.parse(last_search_date_str)
    except Exception as e:
        print(f"Error parsing date '{last_search_date_str}': {e}")
        print("Please use a valid ISO 8601 string or 'YYYY-MM-DD' format.")
        return None

    print(f"🔎 Searching execution history for DAGs owned by '{owner_pattern}' since {last_search_date}...\n")

    # 1. Find DAGs with the specific owner
    dags = session.query(DagModel).filter(DagModel.owners.like(f"%{owner_pattern}%")).all()
    if not dags:
        print(f"No DAGs found for owner matching '{owner_pattern}'.")
        return None

    dag_ids = []
    dag_owner_map = {}
    for dag in dags:
        dag_ids.append(dag.dag_id)
        # Find owner string starting with 'P'
        owners_list = [o.strip() for o in (dag.owners or "").split(",")]
        p_owner = next((o for o in owners_list if o.startswith("P")), dag.dag_id)
        dag_owner_map[dag.dag_id] = p_owner

    print(f"Found {len(dag_ids)} DAG(s) belonging to owner '{owner_pattern}'.")

    # 2. Query separate entities to avoid JOIN overhead
    # First, query TaskInstances executed after last_search_date
    tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id.in_(dag_ids),
        TaskInstance.start_date >= last_search_date
    ).order_by(TaskInstance.start_date.desc()).all()

    if not tis:
        print(f"No task execution history found for owner '{owner_pattern}' since {last_search_date_str}.")
        return None

    # We need the distinct (dag_id, run_id) combinations from the fetched TaskInstances
    target_dag_run_keys = set((ti.dag_id, ti.run_id) for ti in tis)
    
    # Next, query DagRuns independently based on those keys
    dag_runs = []
    if target_dag_run_keys:
        # Simplest way is querying by dag_id in chunks, but here we can just query relevant dag_ids
        # and filter in memory if the tuple `in_` is not fully supported by dialect.
        dag_runs_query = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids)).all()
        # Filter down to only the DagRuns that correspond to our task instances
        dag_runs = [dr for dr in dag_runs_query if (dr.dag_id, dr.run_id) in target_dag_run_keys]

    # 3. Load DagBag to extract task code context
    print("📦 Loading DagBag to fetch task code context...")
    dagbag = DagBag()

    dag_run_rows = []
    task_rows = []

    # Process DagRuns
    for dag_run in dag_runs:
        p_owner = dag_owner_map.get(dag_run.dag_id, dag_run.dag_id)
        dag_run_rows.append({
            "p_owner": p_owner,
            "run_id": dag_run.run_id,
            "dag_run_state": dag_run.state,
            "execution_date": dag_run.execution_date,
            "start_date": dag_run.start_date,
            "end_date": dag_run.end_date,
        })

    # Process TaskInstances
    for ti in tis:
        code = "N/A"
        try:
            dag = dagbag.get_dag(ti.dag_id)
            if dag and dag.has_task(ti.task_id):
                task = dag.get_task(ti.task_id)
                if hasattr(task, 'bash_command'):
                    code = str(task.bash_command)
                elif hasattr(task, 'sql'):
                    code = str(task.sql)
                elif hasattr(task, 'python_callable'):
                    import inspect
                    try:
                        code = inspect.getsource(task.python_callable)
                    except Exception:
                        code = f"Function: {task.python_callable.__name__}"
                else:
                    code = f"[Operator: {task.task_type}] No explicitly supported code attribute."
        except Exception as e:
            code = f"Error extracting code: {str(e)}"

        # Construct TaskInstance data
        p_owner = dag_owner_map.get(ti.dag_id, ti.dag_id)
        task_rows.append({
            "p_owner": p_owner,
            "task_id": ti.task_id,
            "run_id": ti.run_id,
            "try_number": ti.try_number,
            "operator": getattr(task, 'task_type', 'Unknown') if 'task' in locals() else "Unknown",
            "task_state": ti.state,
            "start_date": ti.start_date,
            "end_date": ti.end_date,
            "duration_sec": ti.duration,
            "code_content": code
        })

    # Create DataFrames
    df_dag_run = pd.DataFrame(dag_run_rows)
    df_dag_task = pd.DataFrame(task_rows)

    print(f"\n✅ Created df_dag_run with {len(df_dag_run)} rows and df_dag_task with {len(df_dag_task)} rows.")

    # Find next search date from Task Instances
    latest_start_date = df_dag_task['start_date'].max() if not df_dag_task.empty else None
    next_search_date_str = None
    
    if pd.notnull(latest_start_date):
        next_search_date_str = str(latest_start_date)
        print(f"\n[NEXT_SEARCH_DATE] {next_search_date_str}")
    
    return df_dag_run, df_dag_task, next_search_date_str


now = datetime.now()
target_time = now - timedelta(hours=9, minutes=15)
last_search_date = target_time.strftime('%Y-%m-%d %H:%M:%S')

result = search_task_history(last_search_date_str=last_search_date)

if result:
    df_dag_run, df_dag_task, next_search_date = result
    
    print(f"\n[Used Search Date] {last_search_date}")
    
    print("\n=== df_dag_run Preview ===")
    print(df_dag_run.head())
    
    print("\n=== df_dag_task Preview ===")
    print(df_dag_task[['p_owner', 'task_id', 'try_number', 'operator', 'task_state', 'duration_sec']].head())
