#!/usr/bin/env python3
"""
Airflow Scheduled DAGs Lister
Author: jsangmin <jsm20up@gmail.com>

Description:
    This script connects to the Airflow metadata database (via the Airflow API)
    and lists all active, unpaused DAGs along with their schedule intervals (cron expressions).

    이 스크립트는 Airflow 메타데이터 데이터베이스에 연결하여
    활성화되고 일시 중지되지 않은 모든 DAG와 해당 스케줄 간격(cron 표현식)을 나열합니다.
    
Usage:
    python list_scheduled_dags.py [--dag-id <pattern>] [--owner <pattern>]
    Example: python list_scheduled_dags.py --dag-id _CO_ --owner data_team
"""

import sys
import os
import argparse

from airflow.models import DagModel
from airflow.utils.session import provide_session

@provide_session
def list_scheduled_dags(dag_pattern=None, owner_pattern=None, session=None):
    print(f"{'DAG ID':<50} | {'Owner':<20} | {'Schedule Interval (Cron)':<30}")
    print("-" * 106)

    # Query for active and unpaused DAGs
    query = session.query(DagModel).filter(
        DagModel.is_active == True,
        DagModel.is_paused == False
    )

    if dag_pattern:
        query = query.filter(DagModel.dag_id.like(f"%{dag_pattern}%"))
        
    if owner_pattern:
        query = query.filter(DagModel.owners.like(f"%{owner_pattern}%"))

    dags = query.all()

    if not dags:
        print("No active scheduled DAGs found matching the criteria.")
        return

    for dag in dags:
        if not dag.schedule_interval:
            continue
        
        schedule = str(dag.schedule_interval)
        owner = str(dag.owners) if dag.owners else "N/A"
        print(f"{dag.dag_id:<50} | {owner:<20} | {schedule:<30}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List scheduled DAGs.")
    parser.add_argument("--dag-id", dest="dag_pattern", help="Filter by DAG ID (substring match)")
    parser.add_argument("--owner", dest="owner_pattern", help="Filter by Owner (substring match)")
    
    args = parser.parse_args()
    list_scheduled_dags(dag_pattern=args.dag_pattern, owner_pattern=args.owner_pattern)
