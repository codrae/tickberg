"""dim_symbol SCD1 MERGE — 신규 종목 INSERT, 기존 종목 UPDATE."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.silver.dim_symbol_daily import merge_dim_symbol
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


@pytest.fixture
def dim_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.dim_symbol")
    spark.sql("""
      CREATE TABLE local.silver.dim_symbol (
        symbol string, symbol_name string, market string,
        par_value decimal(18,2), shares_outstanding bigint,
        is_active boolean, updated_ts timestamp
      ) USING iceberg PARTITIONED BY (market)
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.dim_symbol"


def _row(symbol, name, market, par, shares, active=True):
    return Row(
        symbol=symbol, symbol_name=name, market=market,
        par_value=Decimal(par), shares_outstanding=shares,
        is_active=active, updated_ts=datetime(2026, 5, 8, 4, 0, 0),
    )


def test_initial_insert(spark, dim_table):
    src = spark.createDataFrame([
        _row("005930", "삼성전자", "KOSPI", "100", 5969782550),
        _row("000660", "SK하이닉스", "KOSPI", "5000", 728002365),
    ])
    merge_dim_symbol(spark, src_df=src, dim_table=dim_table)
    c = spark.sql(f"SELECT count(*) c FROM {dim_table}").collect()[0].c
    assert c == 2


def test_update_existing_symbol(spark, dim_table):
    src1 = spark.createDataFrame([_row("005930", "삼성전자", "KOSPI", "100", 5969782550)])
    merge_dim_symbol(spark, src_df=src1, dim_table=dim_table)
    src2 = spark.createDataFrame([_row("005930", "삼성전자(액면분할)", "KOSPI", "100", 11939565100)])
    merge_dim_symbol(spark, src_df=src2, dim_table=dim_table)
    rows = spark.sql(f"SELECT * FROM {dim_table}").collect()
    assert len(rows) == 1
    assert rows[0].shares_outstanding == 11939565100
    assert rows[0].symbol_name == "삼성전자(액면분할)"


def test_mixed_insert_update(spark, dim_table):
    """기존 1 + 신규 1 = 같이 처리 (INSERT + UPDATE)."""
    initial = spark.createDataFrame([_row("005930", "삼성전자", "KOSPI", "100", 5969782550)])
    merge_dim_symbol(spark, src_df=initial, dim_table=dim_table)

    next_day = spark.createDataFrame([
        _row("005930", "삼성전자", "KOSPI", "100", 6000000000),  # update
        _row("000660", "SK하이닉스", "KOSPI", "5000", 728002365),  # insert
    ])
    merge_dim_symbol(spark, src_df=next_day, dim_table=dim_table)
    by_symbol = {r.symbol: r for r in spark.sql(f"SELECT * FROM {dim_table}").collect()}
    assert by_symbol["005930"].shares_outstanding == 6000000000
    assert by_symbol["000660"].shares_outstanding == 728002365
