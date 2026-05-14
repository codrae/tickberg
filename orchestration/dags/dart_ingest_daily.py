"""DART 공시 일배치 — 06:00 KST MON-FRI.

전 영업일 공시 list 를 Bronze 적재. spark-master 컨테이너의 env (env_file: .env)
에 DART_API_KEY 가 이미 있어 Airflow Variables 불필요.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="dart_ingest_daily",
    default_args={
        "owner": "tickberg",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
    },
    schedule="0 6 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 6, 0),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "dart", "after-hours"],
) as dag:
    BashOperator(
        task_id="dart_disclosure_ingest",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/bronze/dart_disclosure_ingest.py",
            "--symbols", "005930,000660,035420",
        ),
    )
