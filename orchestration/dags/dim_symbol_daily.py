"""dim_symbol weekday MERGE (04:00 KST MON-FRI). Uses KIS REST → SCD1 MERGE.

장이 열리는 영업일 직전 새벽에만 갱신 — 주말엔 KIS 마스터 변화도 0.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="dim_symbol_daily",
    default_args={
        "owner": "tickberg",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": True,
    },
    schedule="0 4 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 4, 0),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "dim", "after-hours"],
) as dag:
    merge_dim = BashOperator(
        task_id="merge_dim_symbol",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/dim_symbol_daily.py",
            "--symbols", "005930,000660,035420",
            "--dim-table", "glue.tickberg.silver_dim_symbol",
        ),
        env={
            "KIS_APP_KEY": "{{ var.value.KIS_APP_KEY }}",
            "KIS_APP_SECRET": "{{ var.value.KIS_APP_SECRET }}",
            "KIS_BASE_URL": "{{ var.value.get('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443') }}",
        },
    )
