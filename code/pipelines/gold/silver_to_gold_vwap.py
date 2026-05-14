"""Silver → Gold: 1-minute VWAP/OHLC, hour partition OVERWRITE.

Airflow trigger: 5분마다. 현재 hour 파티션만 dynamic overwrite.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

KST = ZoneInfo("Asia/Seoul")


def _hour_filter(hour_start: datetime) -> Column:
    """KST 시각 컴포넌트(year/month/day/hour) 추출 비교로 hour 파티션 필터.

    `trade_ts_kst` 는 Iceberg 카탈로그에 따라 Spark 타입이 갈린다 — Athena DDL
    로 만든 프로덕션 테이블은 `timestamp_ntz`, Spark `CREATE TABLE` 로 만든
    테스트 fixture 는 `timestamp`(TZ). `F.lit(datetime)`(timestamp TZ) 이나
    SQL `timestamp '...'` 리터럴은 둘 중 한쪽 타입과만 정확히 비교돼 다른 쪽은
    0 rows. year/month/day/hour 추출 비교는 두 타입 모두에서 동일하게 동작
    (session.timeZone=Asia/Seoul 전제 — main() 에서 설정).
    """
    c = F.col("trade_ts_kst")
    return (
        (F.year(c) == hour_start.year)
        & (F.month(c) == hour_start.month)
        & (F.dayofmonth(c) == hour_start.day)
        & (F.hour(c) == hour_start.hour)
    )


def compute_vwap_for_hour(
    spark: SparkSession, *, silver_df: DataFrame, hour_kst: datetime
) -> DataFrame:
    hour_start = hour_kst.replace(minute=0, second=0, microsecond=0)

    df = (
        silver_df
        .where(_hour_filter(hour_start))
        .withColumn("ts_minute", F.date_trunc("minute", "trade_ts_kst"))
    )

    w_open = Window.partitionBy("symbol", "ts_minute").orderBy("trade_ts_kst")
    w_close = Window.partitionBy("symbol", "ts_minute").orderBy(F.col("trade_ts_kst").desc())

    enriched = (
        df
        .withColumn("open_price", F.first("price").over(w_open))
        .withColumn("close_price", F.first("price").over(w_close))
    )

    return (
        enriched.groupBy("symbol", "ts_minute")
        .agg(
            F.first("open_price").alias("open_price"),
            F.first("close_price").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.sum("volume").alias("total_volume"),
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume"))
                .cast("decimal(18,4)").alias("vwap"),
            F.count("*").cast("int").alias("trade_count"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("symbol", "ts_minute")
    )


def _current_hour_kst() -> datetime:
    return datetime.now(KST).replace(minute=0, second=0, microsecond=0, tzinfo=None)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--hour", default=None,
                   help="ISO hour KST (yyyy-MM-ddTHH); default = current")
    p.add_argument("--silver-table", default="glue.tickberg.silver_kis_tick_clean")
    p.add_argument("--gold-table", default="glue.tickberg.gold_symbol_vwap_1m")
    args = p.parse_args()

    hour = (
        datetime.fromisoformat(args.hour) if args.hour else _current_hour_kst()
    ).replace(minute=0, second=0, microsecond=0)

    # session.timeZone=Asia/Seoul — streaming job 과 일치 필수. trade_ts_kst 는
    # KST 벽시계를 표현하므로 hour 필터·date_trunc 가 KST 기준으로 동작해야 함.
    # 미설정(UTC default) 시 hour 필터가 9시간 어긋나 0 rows.
    spark = (
        SparkSession.builder.appName("silver_to_gold_vwap")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # hour 필터는 compute_vwap_for_hour 내부에서 적용 — 여기서 중복 X.
    silver = spark.table(args.silver_table)
    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=hour)
    out.writeTo(args.gold_table).overwritePartitions()
    print(f"gold overwrite hour={hour.isoformat()} rows={out.count()}")


if __name__ == "__main__":
    main()
