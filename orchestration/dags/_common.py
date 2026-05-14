"""DAG-common helpers.

Airflow 컨테이너에서 docker.sock 을 통해 spark-master 컨테이너의 spark-submit
을 실행하는 BashOperator command 를 빌드. Airflow image 가 docker-cli 를
포함하고 있어 호스트의 다른 컨테이너에 명령 전송 가능.
"""
from __future__ import annotations

# --total-executor-cores 1: Worker 가 4 cores — streaming app 이 2 cores 상주.
# batch 를 2 cores 로 두면 bronze→silver 와 silver→gold 가 동시 실행 불가
# (4 = streaming 2 + batch 2, 남는 게 0) → silver→gold 의 ExternalTaskSensor
# 가 cores 못 받아 timeout → 연쇄 실패. 1 core 로 낮춰 둘이 동시 실행 가능
# (4 = streaming 2 + bronze→silver 1 + silver→gold 1). partition pruning
# 수정과 합쳐 1 core 여도 batch 가 수십초에 완료.
SPARK_SUBMIT_PREFIX = (
    "docker exec tickberg-spark-master spark-submit "
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--conf spark.driver.host=tickberg-spark-master "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.scheduler.mode=FAIR "
    "--conf spark.scheduler.allocation.file=/opt/bitnami/spark/conf/fairscheduler.xml "
    "--executor-memory 2g --total-executor-cores 1 "
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
