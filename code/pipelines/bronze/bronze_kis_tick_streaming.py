"""Spark Structured Streaming: Kafka kis.tick.raw → S3 Bronze Parquet.

Trigger 1min, append, partition (dt, hr). Long-running outside Airflow.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType,
)

BUCKET = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
TOPIC = os.environ.get("KAFKA_TOPIC_TICK", "kis.tick.raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

OUTPUT_PATH = f"s3a://{BUCKET}/bronze/kis_tick_raw/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/bronze_kis_tick/"


_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_ts_kst", StringType()),
    StructField("price", StringType()),
    StructField("trade_side", StringType()),
    StructField("volume", LongType()),
    StructField("best_ask_price", StringType()),
    StructField("best_bid_price", StringType()),
    StructField("cum_volume", LongType()),
    StructField("cum_amount", LongType()),
    StructField("raw_payload", StringType()),
])


def build_query(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), _SCHEMA).alias("j"),
            F.current_timestamp().alias("ingest_ts"),
        )
        .select(
            "ingest_ts", "kafka_partition", "kafka_offset",
            F.col("j.symbol").alias("symbol"),
            F.to_timestamp("j.trade_ts_kst").alias("trade_ts_kst"),
            F.col("j.price").cast(DecimalType(18, 2)).alias("price"),
            F.col("j.volume").alias("volume"),
            F.col("j.cum_volume").alias("cum_volume"),
            F.col("j.cum_amount").alias("cum_amount"),
            F.col("j.trade_side").alias("trade_side"),
            F.col("j.best_ask_price").cast(DecimalType(18, 2)).alias("best_ask_price"),
            F.col("j.best_bid_price").cast(DecimalType(18, 2)).alias("best_bid_price"),
            F.col("j.raw_payload").alias("raw_payload"),
        )
        .withColumn("dt", F.to_date("trade_ts_kst"))
        .withColumn("hr", F.hour("trade_ts_kst"))
    )

    return (
        parsed.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("dt", "hr")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .queryName("bronze_kis_tick_streaming")
        .start()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("bronze_kis_tick_streaming")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    q = build_query(spark)
    q.awaitTermination()


if __name__ == "__main__":
    main()
