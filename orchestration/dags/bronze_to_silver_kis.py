"""Bronze → Silver MERGE during market hours (KST).

schedule `30 9-15`: 정규장(09:00–15:30 KST) 정렬 — 09:30부터 매시 30분,
마지막 run 15:30 (장 마감). 15:30 이후 빈 배치 run 0회.

Watermark 방식 (`bronze_to_silver_kis_tick.py`) — Silver max(ingest_ts)
부터 새 Bronze 만 읽음. 매 batch I/O 최소, schedule-independent, streaming
catchup spike 자동 흡수. 고정 window 방식과 schedule 의 misalign 으로
데이터 누락이 발생할 수 있었던 문제 (`*/10` → `*/30` → `30 9-15` 변경 시
window-minutes 미조정) 의 근본 해결.
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
    schedule="30 9-15 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "kis", "market-hours"],
) as dag:
    merge = BashOperator(
        task_id="merge_bronze_to_silver",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/bronze_to_silver_kis_tick.py",
            "--silver-table", "glue.tickberg.silver_kis_tick_clean",
            # safety-minutes / fallback-hours 는 default (5 / 12) 사용.
        ),
    )
