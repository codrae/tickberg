"""E2E replay: Bronze fixture → Silver MERGE (dedup) → Gold VWAP (assertion).

Iceberg ① MERGE dedup + ② Gold OVERWRITE 패턴 회귀 안전망. local Hadoop catalog
만으로 외부 의존 없이 풀 pipeline 시연.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DecimalType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from pipelines.gold.silver_to_gold_vwap import compute_vwap_for_hour
from pipelines.silver.bronze_to_silver_kis_tick import merge_bronze_into_silver
from tests.spark_fixtures import spark, warehouse_dir  # noqa: F401


_BRONZE_SCHEMA = StructType([
    StructField("ingest_ts", TimestampType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("symbol", StringType()),
    StructField("trade_ts_kst", TimestampType()),
    StructField("price", DecimalType(18, 2)),
    StructField("volume", LongType()),
    StructField("cum_volume", LongType()),
    StructField("cum_amount", LongType()),
    StructField("trade_side", StringType()),
    StructField("best_ask_price", DecimalType(18, 2)),
    StructField("best_bid_price", DecimalType(18, 2)),
    StructField("raw_payload", StringType()),
])


def _load_fixture(spark, path: Path):
    raw = json.loads(path.read_text())
    rows = [
        (
            datetime.fromisoformat(r["ingest_ts"]),
            r["kafka_partition"], r["kafka_offset"], r["symbol"],
            datetime.fromisoformat(r["trade_ts_kst"]),
            Decimal(r["price"]), r["volume"], r["cum_volume"], r["cum_amount"],
            r["trade_side"],
            Decimal(r["best_ask_price"]), Decimal(r["best_bid_price"]),
            r["raw_payload"],
        )
        for r in raw
    ]
    return spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)


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


def test_e2e_replay_dedup_then_vwap(spark, silver_table):
    """Bronze 3 row (1 중복) → Silver MERGE 2 row → Gold VWAP·OHLC 계산 검증."""
    fixture = Path(__file__).parent / "fixtures" / "bronze_sample.json"
    bronze = _load_fixture(spark, fixture)
    assert bronze.count() == 3, "fixture 3 row 로딩 실패"

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)
    silver_count = spark.sql(f"SELECT count(*) AS c FROM {silver_table}").collect()[0].c
    assert silver_count == 2, f"중복 trade_uid 1건 dedup 후 2 row 기대 — 실제 {silver_count}"

    silver_df = spark.table(silver_table)
    gold = compute_vwap_for_hour(
        spark, silver_df=silver_df, hour_kst=datetime(2026, 5, 8, 9, 0, 0)
    )

    rows = {(r.symbol, r.ts_minute.minute): r for r in gold.collect()}
    assert ("005930", 30) in rows, "09:30 minute bucket 없음"

    g = rows[("005930", 30)]
    expected_vwap = (Decimal("72500") * 100 + Decimal("72600") * 200) / 300  # 72566.66...
    assert abs(g.vwap - expected_vwap) < Decimal("0.01"), \
        f"VWAP 불일치: expected={expected_vwap}, got={g.vwap}"
    assert g.total_volume == 300, f"total_volume 300 기대 — 실제 {g.total_volume}"
    assert g.trade_count == 2, f"trade_count 2 기대 — 실제 {g.trade_count}"
    assert g.high_price == Decimal("72600.00"), f"high {g.high_price}"
    assert g.low_price == Decimal("72500.00"), f"low {g.low_price}"
