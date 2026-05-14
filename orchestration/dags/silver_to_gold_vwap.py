"""Silver → Gold 1m VWAP, hour partition OVERWRITE.

Chained to bronze_to_silver_kis via ExternalTaskSensor (same execution_date).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from _common import spark_submit_command

default_args = {
    "owner": "tickberg",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    dag_id="silver_to_gold_vwap",
    default_args=default_args,
    schedule="30 9-15 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "kis", "market-hours"],
) as dag:
    wait_silver = ExternalTaskSensor(
        task_id="wait_for_bronze_to_silver",
        external_dag_id="bronze_to_silver_kis",
        external_task_id="merge_bronze_to_silver",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 25,  # bronze→silver ~20min (small-files + 1-core) + 5min 마진.
    )

    overwrite_gold = BashOperator(
        task_id="overwrite_gold_vwap",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/gold/silver_to_gold_vwap.py",
            "--silver-table", "glue.tickberg.silver_kis_tick_clean",
            "--gold-table", "glue.tickberg.gold_symbol_vwap_1m",
        ),
    )

    wait_silver >> overwrite_gold
