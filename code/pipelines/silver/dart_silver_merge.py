"""DART Bronze → Silver MERGE — dedup by rcept_no, classify report_kind.

Airflow 호출: spark-submit ... --days-back 2
Test 호출: merge_dart_bronze_into_silver(spark, bronze_df=..., silver_table='local....')

Bronze 는 Parquet (S3 path 직접 read), Silver 는 Iceberg (Glue 카탈로그 MERGE).
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# 분류 룰 — Python 함수와 Spark expr 양쪽에서 참조하는 single source of truth.
# 매칭 순서대로 첫 hit. 마지막은 기본 "기타".
_REPORT_RULES: list[tuple[str, str]] = [
    ("분기보고서", "분기"),
    ("반기보고서", "반기"),
    ("사업보고서", "사업"),
]


def classify_report_kind(report_nm: str | None) -> str:
    """DART report_nm 을 4 클래스로 분류 — 분기 / 반기 / 사업 / 기타."""
    if not report_nm:
        return "기타"
    for needle, label in _REPORT_RULES:
        if needle in report_nm:
            return label
    return "기타"


def _classify_expr(report_nm_col: Column) -> Column:
    """Native Spark expression mirroring classify_report_kind (no UDF).

    UDF 회피 이유: executor 가 driver 의 모듈(`pipelines.silver.dart_silver_merge`)
    을 import 해야 해서 --py-files 또는 addPyFile 필요. native expr 은 직렬화·
    import 부담 없음 + ~10x 빠름. 룰 변경 시 _REPORT_RULES 한 곳만 수정하면
    두 경로(Python·Spark) 모두 동기화.
    """
    safe = F.coalesce(report_nm_col, F.lit(""))
    chain = None
    for needle, label in _REPORT_RULES:
        cond = safe.contains(needle)
        chain = F.when(cond, label) if chain is None else chain.when(cond, label)
    assert chain is not None  # _REPORT_RULES 최소 1개 보장
    return chain.otherwise("기타")


def _enrich(bronze_df: DataFrame) -> DataFrame:
    """Add rcept_date (date), report_kind, silver_ts; drop bronze-only cols.

    rcept_dt 는 'YYYYMMDD' string — time-of-day 정보 없음 → DATE 타입이
    의미 보존 + Iceberg `PARTITIONED BY (rcept_date)` 로 timezone 모호성 회피.
    """
    return (
        bronze_df
        .withColumn("rcept_date", F.to_date(F.col("rcept_dt"), "yyyyMMdd"))
        .withColumn("report_kind", _classify_expr(F.col("report_nm")))
        .withColumn("silver_ts", F.current_timestamp())
        .select(
            "rcept_no", "stock_code", "corp_name", "report_nm",
            "report_kind", "rcept_date", "flr_nm", "silver_ts",
        )
    )


def merge_dart_bronze_into_silver(
    spark: SparkSession, *, bronze_df: DataFrame, silver_table: str
) -> int:
    """Idempotent MERGE on rcept_no. Returns staged row count after source dedup."""
    enriched = _enrich(bronze_df).dropDuplicates(["rcept_no"])
    enriched.createOrReplaceTempView("_dart_bronze_stage")

    spark.sql(f"""
      MERGE INTO {silver_table} t
      USING (SELECT * FROM _dart_bronze_stage) s
      ON  t.rcept_no = s.rcept_no
      WHEN NOT MATCHED THEN INSERT *
    """)
    return enriched.count()


def _read_bronze_window(spark: SparkSession, bucket: str, days_back: int) -> DataFrame:
    """Bronze (Parquet, non-Iceberg) 를 S3 path 로 직접 read.

    `dt` 파티션 컬럼이 자동 discovery 되며, 최근 N일치를 필터.
    """
    bronze_path = f"s3a://{bucket}/bronze/dart_disclosure_raw/"
    return (
        spark.read.parquet(bronze_path)
        .where(F.col("dt") >= F.current_date() - F.expr(f"INTERVAL {days_back} DAYS"))
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days-back", type=int, default=2)
    p.add_argument("--silver-table", default="glue.tickberg.silver_dart_disclosure_clean")
    args = p.parse_args()

    bucket = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
    spark = SparkSession.builder.appName("dart_silver_merge").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")

    bronze = _read_bronze_window(spark, bucket=bucket, days_back=args.days_back)
    n = merge_dart_bronze_into_silver(
        spark, bronze_df=bronze, silver_table=args.silver_table
    )
    print(f"dart silver merged rows={n} days_back={args.days_back} table={args.silver_table}")


if __name__ == "__main__":
    main()
