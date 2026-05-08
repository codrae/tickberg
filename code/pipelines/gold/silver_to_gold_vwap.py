"""Silver → Gold: 1-minute VWAP/OHLC, hour partition OVERWRITE.

Airflow trigger: 5분마다. 현재 hour 파티션만 dynamic overwrite.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

KST = ZoneInfo("Asia/Seoul")


def compute_vwap_for_hour(
    spark: SparkSession, *, silver_df: DataFrame, hour_kst: datetime
) -> DataFrame:
    hour_start = hour_kst.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    df = (
        silver_df
        .where((F.col("trade_ts_kst") >= F.lit(hour_start))
               & (F.col("trade_ts_kst") < F.lit(hour_end)))
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

    spark = SparkSession.builder.appName("silver_to_gold_vwap").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    silver = (
        spark.table(args.silver_table)
        .where((F.col("trade_ts_kst") >= F.lit(hour))
               & (F.col("trade_ts_kst") < F.lit(hour + timedelta(hours=1))))
    )
    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=hour)
    out.writeTo(args.gold_table).overwritePartitions()
    print(f"gold overwrite hour={hour.isoformat()} rows={out.count()}")


if __name__ == "__main__":
    main()
