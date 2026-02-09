#!/usr/bin/env python3
"""
Airflow Task Failure Analyzer
Author: jsangmin <jsm20up@gmail.com>

Description:
    Analyzes task failures over the last 30 days.
    Identifies the "Top N" tasks with the most failures to help pinpoint flaky or problematic tasks.

Usage:
    python analyze_task_failures.py
"""

import sys
from datetime import datetime, timedelta
from sqlalchemy import func, desc

from airflow.models import TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State

def analyze_task_failures(days=30, top_n=20):
    start_date = datetime.now() - timedelta(days=days)
    print(f"Analyzing Task Failures since: {start_date.date()} (Last {days} days)")
    print(f"{'DAG ID':<40} | {'Task ID':<40} | {'Failure Count':<15}")
    print("-" * 100)

    session = create_session()
    try:
        # Query for failed task instances
        # Group by DAG ID and Task ID to find the most frequent offenders
        results = session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            func.count(TaskInstance.task_id).label('count')
        ).filter(
            TaskInstance.start_date >= start_date,
            TaskInstance.state.in_([State.FAILED, State.UP_FOR_RETRY])
        ).group_by(
            TaskInstance.dag_id,
            TaskInstance.task_id
        ).order_by(
            desc('count')
        ).limit(top_n).all()

        if not results:
            print("No task failures found in the specified period.")
            return

        for dag_id, task_id, count in results:
            print(f"{dag_id:<40} | {task_id:<40} | {count:<15}")

    finally:
        session.close()

if __name__ == "__main__":
    analyze_task_failures()
