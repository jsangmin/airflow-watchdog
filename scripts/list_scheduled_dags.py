#!/usr/bin/env python3
"""
Airflow Scheduled DAGs Lister
Author: jsangmin <jsm20up@gmail.com>

Description:
    This script connects to the Airflow metadata database (via the Airflow API)
    and lists all active, unpaused DAGs along with their schedule intervals (cron expressions).
    
Usage:
    python list_scheduled_dags.py
"""

import sys
import os

from airflow.models import DagModel
from airflow.utils.session import create_session

def list_scheduled_dags():
    print(f"{'DAG ID':<50} | {'Schedule Interval (Cron)':<30}")
    print("-" * 83)

    session = create_session()
    try:
        # Query for active and unpaused DAGs
        dags = session.query(DagModel).filter(
            DagModel.is_active == True,
            DagModel.is_paused == False
        ).all()

        if not dags:
            print("No active scheduled DAGs found.")
            return

        for dag in dags:
            if not dag.schedule_interval:
                continue
            
            schedule = str(dag.schedule_interval)
            print(f"{dag.dag_id:<50} | {schedule:<30}")
    finally:
        session.close()

if __name__ == "__main__":
    list_scheduled_dags()
