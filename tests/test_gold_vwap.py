"""1-min VWAP/OHLC aggregation per symbol."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row

from pipelines.gold.silver_to_gold_vwap import compute_vwap_for_hour
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


def _silver_row(symbol, trade_ts, price, volume):
    return Row(
        trade_uid=f"{symbol}_{trade_ts.strftime('%Y%m%d%H%M%S')}_{volume}",
        symbol=symbol, trade_ts_kst=trade_ts,
        trade_ts_utc=trade_ts,
        price=Decimal(price), volume=volume,
        trade_amount=Decimal(price) * volume,
        trade_side="+", best_ask_price=Decimal(price),
        best_bid_price=Decimal(price), ingest_ts=trade_ts, silver_ts=trade_ts,
    )


def test_vwap_aggregates_by_minute(spark):
    rows = [
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 5), "72500", 100),
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 30), "72600", 200),
        _silver_row("005930", datetime(2026, 5, 8, 9, 31, 0), "72700", 50),
    ]
    silver = spark.createDataFrame(rows)

    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=datetime(2026, 5, 8, 9, 0, 0))
    rows_out = {r.ts_minute.minute: r for r in out.collect()}

    assert rows_out[30].total_volume == 300
    assert rows_out[30].open_price == Decimal("72500.00")
    assert rows_out[30].close_price == Decimal("72600.00")
    assert rows_out[30].high_price == Decimal("72600.00")
    assert rows_out[30].low_price == Decimal("72500.00")
    expected_vwap = (Decimal("72500") * 100 + Decimal("72600") * 200) / 300
    assert abs(rows_out[30].vwap - expected_vwap) < Decimal("0.01")
    assert rows_out[30].trade_count == 2
    assert rows_out[31].total_volume == 50


def test_vwap_filters_outside_hour(spark):
    """hour 범위 밖 row 는 무시."""
    rows = [
        _silver_row("005930", datetime(2026, 5, 8, 8, 59, 59), "70000", 1000),  # 이전 hour
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 5), "72500", 100),
        _silver_row("005930", datetime(2026, 5, 8, 10, 0, 0), "73000", 1000),   # 다음 hour
    ]
    silver = spark.createDataFrame(rows)

    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=datetime(2026, 5, 8, 9, 0, 0))
    collected = out.collect()

    assert len(collected) == 1
    assert collected[0].ts_minute.hour == 9
    assert collected[0].total_volume == 100


def test_multi_symbol_independent_aggregation(spark):
    rows = [
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 5), "72500", 100),
        _silver_row("000660", datetime(2026, 5, 8, 9, 30, 5), "120000", 50),
    ]
    silver = spark.createDataFrame(rows)

    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=datetime(2026, 5, 8, 9, 0, 0))
    by_symbol = {r.symbol: r for r in out.collect()}

    assert by_symbol["005930"].total_volume == 100
    assert by_symbol["000660"].total_volume == 50
