"""Local Spark session fixture — Hadoop catalog (no AWS).

이 fixture 는 spark 컨테이너 안에서 pytest 로 실행됨. 컨테이너의
$SPARK_HOME/jars 에 iceberg-spark-runtime, iceberg-aws-bundle 가 이미
존재하므로 spark.jars.packages 옵션은 사용하지 않음.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def warehouse_dir():
    d = tempfile.mkdtemp(prefix="tickberg-iceberg-")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(warehouse_dir):
    s = (
        SparkSession.builder
        .appName("tickberg-tests")
        .master("local[2]")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse_dir))
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.catalog.glue", "")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
