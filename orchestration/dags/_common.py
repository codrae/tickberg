"""DAG-common helpers.

Airflow 컨테이너에서 docker.sock 을 통해 spark-master 컨테이너의 spark-submit
을 실행하는 BashOperator command 를 빌드. Airflow image 가 docker-cli 를
포함하고 있어 호스트의 다른 컨테이너에 명령 전송 가능.
"""
from __future__ import annotations

SPARK_SUBMIT_PREFIX = (
    "docker exec tickberg-spark-master spark-submit "
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--conf spark.driver.host=tickberg-spark-master "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.scheduler.mode=FAIR "
    "--conf spark.scheduler.allocation.file=/opt/bitnami/spark/conf/fairscheduler.xml "
    "--executor-memory 2g --total-executor-cores 2 "
)


def spark_submit_command(script: str, *args: str) -> str:
    """Build BashOperator command for triggering Spark batch in spark-master container.

    Example:
      spark_submit_command(
        '/opt/spark/code/pipelines/silver/bronze_to_silver_kis_tick.py',
        '--window-minutes', '10',
      )
    """
    quoted_args = " ".join(f"'{a}'" for a in args)
    return f"set -euo pipefail; {SPARK_SUBMIT_PREFIX}{script} {quoted_args}"
