"""Spark batch job 공통 SparkSession 빌더.

모든 batch job (bronze/silver/gold) 이 동일한 session 설정을 쓰도록 단일화.
특히 `spark.sql.session.timeZone=Asia/Seoul` 을 강제 — job 마다 tz 설정이
제각각이면 `trade_ts_kst` 가 9시간 어긋나는 버그가 난다 (2026-05-14 timezone
사고, `docs/troubleshooting/2026-05-14-phase1b-operations.md` §1).

import 패턴: batch job 의 `main()` 안에서 sibling import (spark-submit 시
script 디렉터리 기준 sys.path 조정). pytest 는 main() 을 호출하지 않으므로
영향 없음.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def build_batch_session(app_name: str) -> SparkSession:
    """배치 job 용 SparkSession — KST timezone + WARN 로그 + FAIR batch_pool.

    streaming job (`bronze_kis_tick_streaming`) 과 timeZone 일치 (Asia/Seoul).
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    return spark
