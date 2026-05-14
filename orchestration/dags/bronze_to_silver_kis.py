"""Bronze → Silver MERGE during market hours (KST).

schedule */30: bronze→silver job 이 Bronze small-files 누적 + 1-core 자원
제약으로 ~20min 소요 → */10 은 backlog 영구 적체. */30 으로 10min margin.
근본 수정 (Bronze compaction / hr-level pruning) 은 Phase 2.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

default_args = {
    "owner": "tickberg",
    "depends_on_past": False,

    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=10),
}

with DAG(
    dag_id="bronze_to_silver_kis",
    default_args=default_args,
    description="Merge recent Bronze KIS ticks into Silver (dedup by trade_uid)",
    schedule="*/30 9-16 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "kis", "market-hours"],
) as dag:
    merge = BashOperator(
        task_id="merge_bronze_to_silver",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/bronze_to_silver_kis_tick.py",
            "--window-minutes", "15",
            "--silver-table", "glue.tickberg.silver_kis_tick_clean",
        ),
    )
