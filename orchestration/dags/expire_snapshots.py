"""Iceberg expire_snapshots — 주 1회 일요일 19:00 KST.

Iceberg 자동화 #2 (Compaction 과 짝). 30일 이전 snapshot 삭제,
최소 5 snapshot 보존. 장 마감 후 retention window.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="expire_snapshots",
    default_args={
        "owner": "tickberg",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": True,
    },
    schedule="0 19 * * SUN",
    start_date=datetime(2026, 5, 17, 19, 0),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "iceberg", "after-hours"],
) as dag:
    BashOperator(
        task_id="expire_snapshots",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/expire_snapshots.py",
            "--retention-days", "30",
            "--retain-last", "5",
        ),
    )
