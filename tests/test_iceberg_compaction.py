"""Compaction의 file 수 감소 동작 검증 (small Spark + Hadoop catalog).

Iceberg Spark Action `rewrite_data_files` 는 SQL stored procedure 로도 호출 가능.
"""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row

from pipelines.silver.iceberg_compaction import compact_table
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


@pytest.fixture
def small_files_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.small_files")
    spark.sql("""
      CREATE TABLE local.silver.small_files (
        id bigint, payload string, ts timestamp
      ) USING iceberg PARTITIONED BY (days(ts))
      TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='268435456')
    """)
    base_ts = datetime(2026, 5, 8, 9, 0, 0)
    for i in range(10):
        rows = [Row(id=i*100+j, payload="x"*100, ts=base_ts) for j in range(100)]
        spark.createDataFrame(rows).writeTo("local.silver.small_files").append()
    yield "local.silver.small_files"


def test_compaction_reduces_file_count(spark, small_files_table):
    files_before = spark.sql(f"SELECT count(*) c FROM {small_files_table}.files").collect()[0].c
    assert files_before > 1
    compact_table(spark, table=small_files_table, target_file_size_bytes=384*1024*1024)
    files_after = spark.sql(f"SELECT count(*) c FROM {small_files_table}.files").collect()[0].c
    assert files_after < files_before
