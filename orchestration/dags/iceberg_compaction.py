"""Iceberg compaction (18:00 KST MON-FRI) on Silver + Gold."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="iceberg_compaction",
    default_args={
        "owner": "tickberg",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
    },
    schedule="0 18 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 18, 0),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "after-hours"],
) as dag:
    compact = BashOperator(
        task_id="rewrite_data_files",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/iceberg_compaction.py",
            "--tables",
            "glue.tickberg.silver_kis_tick_clean",
            "glue.tickberg.gold_symbol_vwap_1m",
            "--target-mb", "384",
        ),
    )
