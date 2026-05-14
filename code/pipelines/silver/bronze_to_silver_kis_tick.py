"""Bronze → Silver MERGE INTO with synthetic trade_uid dedup.

Airflow 호출: spark-submit ... --window-minutes 10
Test 호출: merge_bronze_into_silver(spark, bronze_df=..., silver_table='local....')
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KST = ZoneInfo("Asia/Seoul")


def _enrich(bronze_df: DataFrame) -> DataFrame:
    """Add trade_uid + trade_ts_utc + trade_amount + silver_ts."""
    return (
        bronze_df
        .withColumn(
            "trade_uid",
            F.concat_ws(
                "_",
                F.col("symbol"),
                F.date_format("trade_ts_kst", "yyyyMMddHHmmss"),
                F.col("cum_volume").cast("string"),
            ),
        )
        .withColumn(
            "trade_ts_utc",
            F.from_utc_timestamp(F.to_utc_timestamp("trade_ts_kst", "Asia/Seoul"), "UTC"),
        )
        .withColumn(
            "trade_amount",
            (F.col("price") * F.col("volume")).cast("decimal(20,2)"),
        )
        .withColumn("silver_ts", F.current_timestamp())
        .select(
            "trade_uid", "symbol", "trade_ts_kst", "trade_ts_utc",
            "price", "volume", "trade_amount", "trade_side",
            "best_ask_price", "best_bid_price", "ingest_ts", "silver_ts",
        )
    )


def merge_bronze_into_silver(
    spark: SparkSession, *, bronze_df: DataFrame, silver_table: str
) -> int:
    """Idempotent MERGE on trade_uid. Return staged-row count (best effort).

    같은 micro-batch 내 중복 trade_uid 도 처리하기 위해 source 단계에서
    dropDuplicates 한다 — Iceberg MERGE 는 source 내부 중복은 자동 dedup
    하지 않음 (multi-match 로 둘 다 INSERT 또는 에러).
    """
    enriched = _enrich(bronze_df).dropDuplicates(["trade_uid"])
    enriched.createOrReplaceTempView("_bronze_stage")

    spark.sql(f"""
      MERGE INTO {silver_table} t
      USING (SELECT * FROM _bronze_stage) s
      ON  t.trade_uid = s.trade_uid
      WHEN NOT MATCHED THEN INSERT *
    """)
    return enriched.count()


def _read_bronze_window(spark: SparkSession, bucket: str, window_minutes: int) -> DataFrame:
    """Read recent N minutes of Bronze with dt partition pruning.

    Bronze 는 Parquet (dt, hr) 파티션 — Iceberg-only `glue` 카탈로그로 접근
    불가하여 S3 path 로 직접 read. `ingest_ts` 만으로 필터하면 partition
    컬럼이 아니라 pruning 이 안 되어 Bronze 전체 history (수일치 small-files)
    를 매번 listing/read → O(전체 history). 먼저 `dt` 파티션 컬럼으로 좁힌 뒤
    `ingest_ts` 정밀 필터를 적용한다. window 가 날짜 경계를 넘거나 late
    arrival 을 포착하도록 1일 buffer 를 둔다.
    """
    lookback_date = (datetime.now(KST) - timedelta(minutes=window_minutes, days=1)).date()
    bronze_path = f"s3a://{bucket}/bronze/kis_tick_raw/"
    return (
        spark.read.parquet(bronze_path)
        .where(F.col("dt") >= F.lit(lookback_date))
        .where(
            F.col("ingest_ts")
            >= F.current_timestamp() - F.expr(f"INTERVAL {window_minutes} MINUTES")
        )
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--window-minutes", type=int, default=10)
    p.add_argument("--silver-table", default="glue.tickberg.silver_kis_tick_clean")
    args = p.parse_args()

    bucket = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from _spark import build_batch_session

    spark = build_batch_session("bronze_to_silver_kis_tick")

    bronze = _read_bronze_window(spark, bucket=bucket, window_minutes=args.window_minutes)
    n = merge_bronze_into_silver(
        spark, bronze_df=bronze, silver_table=args.silver_table
    )
    print(f"merged window={args.window_minutes}min rows={n} table={args.silver_table}")


if __name__ == "__main__":
    main()
