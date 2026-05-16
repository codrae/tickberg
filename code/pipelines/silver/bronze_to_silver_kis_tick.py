"""Bronze → Silver MERGE INTO with synthetic trade_uid dedup.

watermark 방식 — Silver 의 `max(ingest_ts)` 부터 새 Bronze 만 읽음.
schedule-independent (cron 바뀌어도 작동), I/O 최소, streaming catchup spike
자동 흡수. Iceberg `max(ingest_ts)` 는 manifest stats 로 즉시 응답.

Airflow 호출: spark-submit ... --safety-minutes 5
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


def _watermark(spark: SparkSession, silver_table: str,
               safety_minutes: int, fallback_hours: int) -> datetime:
    """Silver 의 max(ingest_ts) − safety_minutes. 빈 테이블이면 fallback."""
    row = spark.sql(f"SELECT MAX(ingest_ts) AS m FROM {silver_table}").collect()
    m = row[0].m if row and row[0].m is not None else None
    if m is None:
        # 첫 run — Silver 비어있음. fallback_hours lookback.
        return (datetime.now(KST) - timedelta(hours=fallback_hours)).replace(tzinfo=None)
    return m - timedelta(minutes=safety_minutes)


def _read_bronze_window(
    spark: SparkSession, *, bucket: str, silver_table: str,
    safety_minutes: int = 5, fallback_hours: int = 12,
    window_minutes_override: int | None = None,
) -> DataFrame:
    """Watermark 기반 — Silver 의 max(ingest_ts) 이후 Bronze 만 read.

    schedule-independent: cron 변경(매시/매분/일배치)에도 정합 유지.
    매 batch I/O = "정확히 새로 들어온 만큼" + safety_minutes 재읽기 (MERGE
    idempotent 라 안전). Iceberg max() 는 manifest stats 로 cheap.

    `window_minutes_override` 가 주어지면 watermark 무시하고 `ingest_ts > now - N min`
    으로 작동 — catchup / 재처리용 escape hatch (watermark 는 historical 갭을
    못 메움; max(silver.ingest_ts) 가 이미 앞서있으면 그 뒤의 누락분도 skip 함).
    """
    if window_minutes_override is not None:
        watermark = (datetime.now(KST) - timedelta(minutes=window_minutes_override)
                     ).replace(tzinfo=None)
    else:
        watermark = _watermark(spark, silver_table, safety_minutes, fallback_hours)
    lookback_date = (watermark - timedelta(days=1)).date()
    bronze_path = f"s3a://{bucket}/bronze/kis_tick_raw/"
    # SQL 문자열 literal 사용 — F.lit(naive datetime) 은 driver process 의
    # system tz 로 해석되는데 spark-master 컨테이너는 TZ=UTC 라 KST 의도와
    # 9 시간 어긋남. `timestamp '...'` SQL 리터럴은 session.timeZone(Asia/Seoul)
    # 으로 해석돼 안전.
    wm_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
    return (
        spark.read.parquet(bronze_path)
        .where(f"dt >= date '{lookback_date}'")
        .where(f"ingest_ts > timestamp '{wm_str}'")
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--safety-minutes", type=int, default=5,
                   help="watermark 뒤 안전 마진 — late arrival 흡수, MERGE idempotent")
    p.add_argument("--fallback-hours", type=int, default=12,
                   help="Silver 비어있을 때 lookback 윈도우 (첫 run·재해 복구)")
    p.add_argument("--window-minutes", type=int, default=None,
                   help="수동 override — 지정 시 watermark 무시하고 now-N min 부터 read. "
                        "catchup·historical 갭 복구용 (watermark 는 갭 못 메움)")
    p.add_argument("--silver-table", default="glue.tickberg.silver_kis_tick_clean")
    args = p.parse_args()

    bucket = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from _spark import build_batch_session

    spark = build_batch_session("bronze_to_silver_kis_tick")

    bronze = _read_bronze_window(
        spark, bucket=bucket, silver_table=args.silver_table,
        safety_minutes=args.safety_minutes, fallback_hours=args.fallback_hours,
        window_minutes_override=args.window_minutes,
    )
    n = merge_bronze_into_silver(
        spark, bronze_df=bronze, silver_table=args.silver_table
    )
    mode = f"window={args.window_minutes}m (override)" if args.window_minutes else f"watermark·safety={args.safety_minutes}m"
    print(f"merged rows={n} table={args.silver_table} ({mode})")


if __name__ == "__main__":
    main()
