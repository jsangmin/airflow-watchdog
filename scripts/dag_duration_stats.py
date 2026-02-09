#!/usr/bin/env python3
"""
Airflow DAG Duration Stats
Author: jsangmin <jsm20up@gmail.com>

Description:
    Calculates and prints the duration of each DAG run for a given DAG ID over the last 30 days.
    Also computes the average duration.

Usage:
    python dag_duration_stats.py <dag_id>
"""

import argparse
from datetime import datetime, timedelta
# Import modules directly, assuming environment is correct. 
# Avoiding sys.exit(1) on import error as requested, though standard python behavior will raise ImportError and exit non-zero anyway if unhandled.
# I will wrap in try-except and print but not sys.exit(1).

try:
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import State
except ImportError as e:
    print(f"Error importing airflow modules: {e}")
    # Proceeding might fail but honoring "no sys.exit(1)" request by just returning/ending naturally or letting python crash.
    # To be safe and clean, I'll return from main if possible.

def get_duration_stats(dag_id):
    print(f"Calculating duration stats for DAG: '{dag_id}' (Last 30 Days)")
    print(f"{'Run ID':<50} | {'Execution Date':<25} | {'Duration (s)':<15} | {'State':<10}")
    print("-" * 108)

    try:
        session = create_session()
    except NameError:
        return # Airflow not imported

    try:
        thirty_days_ago = datetime.now() - timedelta(days=30)
        
        # Query for DagRuns of the given DAG in the last 30 days
        # We include all states to show what happened, but duration is only valid if start_date exists.
        # End_date might be null if running.
        runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date >= thirty_days_ago
        ).order_by(DagRun.execution_date.desc()).all()

        if not runs:
            print(f"No runs found for DAG '{dag_id}' in the last 30 days.")
            return

        total_duration = 0
        count = 0

        for run in runs:
            duration_str = "N/A"
            duration_seconds = 0
            
            if run.start_date and run.end_date:
                duration_obj = run.end_date - run.start_date
                duration_seconds = duration_obj.total_seconds()
                duration_str = f"{duration_seconds:.2f}"
                
                # Only include in average if it finished (success or failed with valid end time)
                total_duration += duration_seconds
                count += 1
            elif run.start_date and not run.end_date:
                duration_str = "Running"
            
            print(f"{run.run_id:<50} | {run.execution_date.isoformat():<25} | {duration_str:<15} | {run.state:<10}")

        print("-" * 108)
        if count > 0:
            avg_duration = total_duration / count
            print(f"Average Duration (finished runs): {avg_duration:.2f} seconds")
        else:
            print("No finished runs to calculate average.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get DAG execution duration stats.")
    parser.add_argument("dag_id", help="The DAG ID to analyze")
    
    args = parser.parse_args()
    get_duration_stats(args.dag_id)
