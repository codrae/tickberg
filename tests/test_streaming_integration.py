"""Spark Structured Streaming 통합 테스트 — Kafka → Parquet end-to-end.

testcontainers 대신 가동 중인 tickberg-kafka 의 격리 토픽 `kis.tick.test` 사용.
ephemeral spark container 가 --network tickberg-net 으로 join 해 kafka:9092 접근.
processAllAvailable() 로 1분 trigger 대기 없이 즉시 micro-batch 발화.

영업시간 의존성 없음 (별도 topic, 자체 publish). 통합 회귀 안전망.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from kafka import KafkaAdminClient, KafkaProducer
from pyspark.sql import SparkSession


KAFKA_BOOTSTRAP = "kafka:9092"  # compose hostname (run inside tickberg-net)
TOPIC = "kis.tick.test"


@pytest.fixture(scope="module")
def kafka_available():
    """Verify the test topic exists; skip if not reachable."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP,
                                 request_timeout_ms=5000)
        topics = admin.list_topics()
        admin.close()
    except Exception as e:
        pytest.skip(f"Kafka not reachable at {KAFKA_BOOTSTRAP}: {e}")
    if TOPIC not in topics:
        pytest.skip(f"test topic {TOPIC} missing — pre-create via kafka-topics.sh")


@pytest.fixture(scope="module")
def streaming_spark():
    """Dedicated SparkSession for streaming test (separate from spark_fixtures.py).

    spark-defaults.conf 의 glue catalog eager init 회피 — default catalog
    를 spark_catalog 로 강제. 테스트는 local parquet path 만 사용.
    """
    spark = (
        SparkSession.builder
        .appName("streaming-it")
        .master("local[2]")
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .config("spark.sql.catalog.glue", "")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def _publish_messages(n: int = 5) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    for i in range(n):
        producer.send(TOPIC, {
            "symbol": "005930",
            "trade_ts_kst": f"2026-05-08T09:30:0{i}",
            "price": str(72500 + i * 100),
            "volume": 100,
            "cum_volume": 1000 + i,
            "cum_amount": 100_000_000 + i,
            "trade_side": "+",
            "best_ask_price": str(72500 + i * 100),
            "best_bid_price": str(72400 + i * 100),
            "raw_payload": "it-test",
        })
    producer.flush()
    producer.close()


def test_kafka_to_parquet_micro_batch(kafka_available, streaming_spark, tmp_path: Path):
    """5 message publish → build_query → processAllAvailable → parquet 5 row 검증."""
    spark = streaming_spark
    out_dir = str(tmp_path / "bronze")
    ckpt_dir = str(tmp_path / "checkpoint")

    from pipelines.bronze import bronze_kis_tick_streaming as m
    m.BOOTSTRAP = KAFKA_BOOTSTRAP
    m.TOPIC = TOPIC
    m.OUTPUT_PATH = out_dir
    m.CHECKPOINT_PATH = ckpt_dir

    # readStream 의 startingOffsets='latest' — 먼저 query 시작해 latest offset
    # 고정 후 publish. consumer 가 새 메시지를 읽음.
    q = m.build_query(spark)
    try:
        time.sleep(3)  # consumer 가 broker 와 fully connect 할 시간
        _publish_messages(5)
        q.processAllAvailable()
    finally:
        q.stop()

    df = spark.read.parquet(out_dir)
    assert df.count() == 5, f"expected 5 rows in Parquet, got {df.count()}"
    symbols = {r.symbol for r in df.select("symbol").distinct().collect()}
    assert symbols == {"005930"}, f"expected only 005930, got {symbols}"
    prices = sorted(int(r.price) for r in df.select("price").collect())
    assert prices == [72500, 72600, 72700, 72800, 72900]
