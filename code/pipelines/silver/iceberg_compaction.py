"""Iceberg compaction — rewrite_data_files SQL stored procedure.

Spec §4.2 — 18:00 KST MON-FRI. Silver/Gold target 384MB (256–512MB band).
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def compact_table(
    spark: SparkSession,
    *,
    table: str,
    target_file_size_bytes: int = 384 * 1024 * 1024,
    min_file_size_bytes: int = 256 * 1024 * 1024,
    max_file_size_bytes: int = 512 * 1024 * 1024,
) -> dict:
    """Run Iceberg `rewrite_data_files` stored procedure on `table`.

    Returns map of metrics from the procedure call (rewritten_data_files_count etc).
    """
    catalog = table.split(".")[0]
    qualified = ".".join(table.split(".")[1:])
    sql = f"""
      CALL {catalog}.system.rewrite_data_files(
        table => '{qualified}',
        options => map(
          'target-file-size-bytes', '{target_file_size_bytes}',
          'min-file-size-bytes', '{min_file_size_bytes}',
          'max-file-size-bytes', '{max_file_size_bytes}',
          'rewrite-all', 'false'
        )
      )
    """
    row = spark.sql(sql).collect()
    return row[0].asDict() if row else {}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--tables", nargs="+",
        default=[
            "glue.tickberg.silver_kis_tick_clean",
            "glue.tickberg.gold_symbol_vwap_1m",
        ],
    )
    p.add_argument("--target-mb", type=int, default=384)
    args = p.parse_args()

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from _spark import build_batch_session

    spark = build_batch_session("iceberg_compaction")
    target = args.target_mb * 1024 * 1024

    for t in args.tables:
        # per-table try/except — 한 테이블 compaction 실패가 나머지를 막지
        # 않도록 (expire_snapshots.py 와 동일 패턴).
        try:
            result = compact_table(spark, table=t, target_file_size_bytes=target)
            print(f"compacted {t}: {result}")
        except Exception as e:  # noqa: BLE001
            print(f"WARN compaction failed for {t}: {e}")


if __name__ == "__main__":
    main()
