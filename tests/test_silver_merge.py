"""Demo prep test: 동일 trade_uid 가 두 번 들어오면 MERGE 후 1건만 적재됨."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.silver.bronze_to_silver_kis_tick import merge_bronze_into_silver
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


def _bronze_row(*, symbol="005930", trade_dt=datetime(2026, 5, 8, 9, 30, 1),
                price="72500", volume=100, cum_volume=1234567):
    return Row(
        ingest_ts=datetime(2026, 5, 8, 0, 30, 5),
        kafka_partition=0, kafka_offset=42,
        symbol=symbol, trade_ts_kst=trade_dt,
        price=Decimal(price), volume=volume,
        cum_volume=cum_volume, cum_amount=cum_volume * int(price),
        trade_side="+", best_ask_price=Decimal(price),
        best_bid_price=Decimal(str(int(price) - 100)),
        raw_payload="dummy",
    )


@pytest.fixture
def silver_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.kis_tick_clean")
    spark.sql("""
      CREATE TABLE local.silver.kis_tick_clean (
        trade_uid string, symbol string,
        trade_ts_kst timestamp, trade_ts_utc timestamp,
        price decimal(18,2), volume bigint, trade_amount decimal(20,2),
        trade_side string, best_ask_price decimal(18,2), best_bid_price decimal(18,2),
        ingest_ts timestamp, silver_ts timestamp
      ) USING iceberg
      PARTITIONED BY (hours(trade_ts_kst))
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.kis_tick_clean"


def test_duplicate_trade_uid_merged_to_one_row(spark, silver_table):
    bronze = spark.createDataFrame([_bronze_row(), _bronze_row()])

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    result = spark.sql(f"SELECT count(*) AS c FROM {silver_table}").collect()[0]
    assert result.c == 1


def test_merge_preserves_distinct_rows(spark, silver_table):
    rows = [
        _bronze_row(cum_volume=1),
        _bronze_row(cum_volume=2),
        _bronze_row(symbol="000660", cum_volume=1),
    ]
    bronze = spark.createDataFrame(rows)

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    out = spark.sql(f"SELECT symbol, count(*) c FROM {silver_table} GROUP BY symbol ORDER BY symbol")
    assert [(r.symbol, r.c) for r in out.collect()] == [("000660", 1), ("005930", 2)]


def test_second_run_with_overlapping_window_idempotent(spark, silver_table):
    bronze = spark.createDataFrame([_bronze_row(cum_volume=10)])
    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)
    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    c = spark.sql(f"SELECT count(*) c FROM {silver_table}").collect()[0].c
    assert c == 1
