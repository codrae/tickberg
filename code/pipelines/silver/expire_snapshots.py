"""Iceberg expire_snapshots — Silver/Gold 의 30일 이전 snapshot 정리.

주 1회 일요일 19:00 KST 실행 (장 마감 후 retention window).
Compaction 과 짝 — file 병합 + metadata 정리 = storage·query 최적화.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _spark import build_batch_session, split_catalog  # noqa: E402


def expire(spark: SparkSession, *, table: str, retention_days: int,
           retain_last: int) -> None:
    """`{catalog}.system.expire_snapshots` procedure 호출.

    Iceberg 가 retain_last 와 older_than 중 보수적인 쪽을 적용 — 즉 retain_last
    개수는 절대 보장. retention_days 가 짧아도 retain_last 5는 유지.
    """
    catalog, qualified = split_catalog(table)
    older_than = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
      CALL {catalog}.system.expire_snapshots(
        table       => '{qualified}',
        older_than  => TIMESTAMP '{older_than}',
        retain_last => {retain_last}
      )
    """)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tables", nargs="+", default=[
        "glue.tickberg.silver_kis_tick_clean",
        "glue.tickberg.silver_dart_disclosure_clean",
        "glue.tickberg.gold_symbol_vwap_1m",
    ])
    p.add_argument("--retention-days", type=int, default=30)
    p.add_argument("--retain-last", type=int, default=5)
    args = p.parse_args()

    spark = build_batch_session("expire_snapshots")

    for t in args.tables:
        try:
            expire(spark, table=t,
                   retention_days=args.retention_days,
                   retain_last=args.retain_last)
            print(f"expired {t} (older than {args.retention_days}d, keep last {args.retain_last})")
        except Exception as e:  # noqa: BLE001
            # 테이블 비어있거나 procedure 일시 fail 도 다음 테이블 진행 — WARN 후 다음.
            print(f"WARN expire failed for {t}: {e}")


if __name__ == "__main__":
    main()
