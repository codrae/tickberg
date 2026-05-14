"""DART Bronze → Silver MERGE: dedup by rcept_no + report_kind 분류."""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import Row

from pipelines.silver.dart_silver_merge import (
    classify_report_kind,
    merge_dart_bronze_into_silver,
)
from tests.spark_fixtures import spark, warehouse_dir  # noqa: F401


@pytest.mark.parametrize("report_nm,expected", [
    ("분기보고서 (2026.03)", "분기"),
    ("반기보고서 (2026.06)", "반기"),
    ("사업보고서 (2025.12)", "사업"),
    ("주요사항보고서(유상증자결정)", "기타"),
    ("최대주주등소유주식변동신고서", "기타"),
    ("", "기타"),
    (None, "기타"),
])
def test_classify_report_kind(report_nm, expected):
    assert classify_report_kind(report_nm) == expected


@pytest.fixture
def silver_dart_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.dart_disclosure_clean")
    spark.sql("""
      CREATE TABLE local.silver.dart_disclosure_clean (
        rcept_no    string,
        stock_code  string,
        corp_name   string,
        report_nm   string,
        report_kind string,
        rcept_date  date,
        flr_nm      string,
        silver_ts   timestamp
      ) USING iceberg
      PARTITIONED BY (rcept_date)
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.dart_disclosure_clean"


def _bronze_row(*, rcept_no, stock_code="005930", corp_name="삼성전자",
                report_nm="분기보고서 (2026.03)", rcept_dt="20260508", flr_nm="삼성전자"):
    return Row(
        rcept_no=rcept_no, corp_code="00126380", corp_name=corp_name,
        stock_code=stock_code, report_nm=report_nm, rcept_dt=rcept_dt,
        flr_nm=flr_nm, rm="",
        ingest_ts=datetime(2026, 5, 14, 6, 0, 0),
        dt=datetime(2026, 5, 8).date(),
    )


def test_initial_insert(spark, silver_dart_table):
    bronze = spark.createDataFrame([
        _bronze_row(rcept_no="20260508001"),
        _bronze_row(rcept_no="20260508002", stock_code="000660", corp_name="SK하이닉스"),
    ])

    n = merge_dart_bronze_into_silver(
        spark, bronze_df=bronze, silver_table=silver_dart_table
    )

    assert n == 2
    rows = spark.sql(
        f"SELECT count(*) c FROM {silver_dart_table}"
    ).collect()
    assert rows[0].c == 2


def test_duplicate_rcept_no_dedup_to_one_row(spark, silver_dart_table):
    bronze = spark.createDataFrame([
        _bronze_row(rcept_no="20260508001"),
        _bronze_row(rcept_no="20260508001"),
    ])

    merge_dart_bronze_into_silver(
        spark, bronze_df=bronze, silver_table=silver_dart_table
    )

    c = spark.sql(
        f"SELECT count(*) c FROM {silver_dart_table}"
    ).collect()[0].c
    assert c == 1


def test_second_run_idempotent(spark, silver_dart_table):
    bronze = spark.createDataFrame([_bronze_row(rcept_no="20260508001")])
    merge_dart_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_dart_table)
    merge_dart_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_dart_table)

    c = spark.sql(f"SELECT count(*) c FROM {silver_dart_table}").collect()[0].c
    assert c == 1


def test_report_kind_classified_in_silver(spark, silver_dart_table):
    bronze = spark.createDataFrame([
        _bronze_row(rcept_no="A", report_nm="분기보고서 (2026.03)"),
        _bronze_row(rcept_no="B", report_nm="반기보고서 (2026.06)"),
        _bronze_row(rcept_no="C", report_nm="사업보고서 (2025.12)"),
        _bronze_row(rcept_no="D", report_nm="주요사항보고서(유상증자결정)"),
    ])

    merge_dart_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_dart_table)

    rows = spark.sql(
        f"SELECT rcept_no, report_kind FROM {silver_dart_table} ORDER BY rcept_no"
    ).collect()
    assert [(r.rcept_no, r.report_kind) for r in rows] == [
        ("A", "분기"), ("B", "반기"), ("C", "사업"), ("D", "기타"),
    ]


def test_rcept_date_parsed_from_rcept_dt_string(spark, silver_dart_table):
    bronze = spark.createDataFrame([_bronze_row(rcept_no="X", rcept_dt="20260508")])

    merge_dart_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_dart_table)

    rows = spark.sql(f"SELECT rcept_date FROM {silver_dart_table}").collect()
    assert rows[0].rcept_date == date(2026, 5, 8)
