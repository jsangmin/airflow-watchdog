#!/usr/bin/env python3
"""
Airflow DAG Search Tool
Author: jsangmin <jsm20up@gmail.com>

Description:
    This script searches for DAGs whose IDs match a given pattern (substring).
    It connects to the Airflow metadata database.

Usage:
    python dag_search.py <search_pattern>
    Example: python dag_search.py _CO_
"""

import argparse
from sqlalchemy import or_

from airflow.models import DagModel
from airflow.utils.session import create_session

def search_dags(pattern):
    print(f"Searching for DAGs containing: '{pattern}'")
    print(f"{'DAG ID':<50} | {'File Path':<50}")
    print("-" * 103)

    session = create_session()
    try:
        # Search for DAGs where dag_id contains the pattern (case-insensitive usually depends on DB collation, using ilike for postgres if needed but like is standard)
        # Using simple 'like' with wildcards for broad compatibility, or specifically filtering.
        # SQLAlchemy's like is usually case-sensitive on some DBs, ilike is case-insensitive on Postgres.
        # Airflow usually uses Postgres or MySQL.
        
        dags = session.query(DagModel).filter(
            DagModel.dag_id.like(f"%{pattern}%")
        ).all()

        if not dags:
            print(f"No DAGs found matching pattern: {pattern}")
            return

        for dag in dags:
            print(f"{dag.dag_id:<50} | {dag.fileloc:<50}")
            
    finally:
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for Airflow DAGs by name pattern.")
    parser.add_argument("pattern", help="The substring to search for in DAG IDs (e.g., '_CO_')")
    
    args = parser.parse_args()
    search_dags(args.pattern)
