#!/usr/bin/env python3
"""
Airflow Stale DAG Detector
Author: jsangmin <jsm20up@gmail.com>

Description:
    Identifies "Stale" DAGs that have not been executed in the last 30 days.
    It distinguishes between Active (should run but doesn't) and Paused DAGs.

Usage:
    python detect_stale_dags.py
"""

import sys
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import func

from airflow.models import DagModel, DagRun
from airflow.utils.session import create_session

def detect_stale_dags(days=30):
    cutoff_date = datetime.now() - timedelta(days=days)
    print(f"Checking for DAGs with no execution since: {cutoff_date.date()}")
    print(f"{'DAG ID':<50} | {'State':<10} | {'Last Run Date':<20}")
    print("-" * 86)

    session = create_session()
    try:
        # Get all DAGs
        dags = session.query(DagModel).all()
        
        stale_count = 0
        
        for dag in dags:
            # Get the latest run for this DAG
            last_run = session.query(func.max(DagRun.execution_date)).filter(
                DagRun.dag_id == dag.dag_id
            ).scalar()
            
            is_stale = False
            last_run_str = "Never"
            
            if last_run:
                if last_run < cutoff_date:
                    is_stale = True
                    last_run_str = last_run.strftime("%Y-%m-%d")
            else:
                # No runs at all
                is_stale = True

            if is_stale:
                status = "Active" if dag.is_active and not dag.is_paused else "Paused"
                # If it's paused, being stale is expected, but checking for "Long Paused"
                print(f"{dag.dag_id:<50} | {status:<10} | {last_run_str:<20}")
                stale_count += 1
        
        if stale_count == 0:
            print("No stale DAGs found.")
        else:
            print("-" * 86)
            print(f"Total Stale DAGs: {stale_count}")

    finally:
        session.close()

if __name__ == "__main__":
    detect_stale_dags()
