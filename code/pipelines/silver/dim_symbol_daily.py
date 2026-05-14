"""dim_symbol daily MERGE (SCD1).

DAG 가 매일 04:00 KST 에 KIS REST 로 종목 메타 조회 후 MERGE INTO.
액면분할/상폐 등 종목 마스터 변경을 silver 에 반영.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, Row, SparkSession

KST = ZoneInfo("Asia/Seoul")


def merge_dim_symbol(spark: SparkSession, *, src_df: DataFrame, dim_table: str) -> None:
    """SCD1 MERGE — 같은 symbol 은 UPDATE, 신규는 INSERT."""
    src_df.createOrReplaceTempView("_dim_src")
    spark.sql(f"""
      MERGE INTO {dim_table} t
      USING (SELECT * FROM _dim_src) s
      ON t.symbol = s.symbol
      WHEN MATCHED THEN UPDATE SET
        symbol_name = s.symbol_name,
        market = s.market,
        par_value = s.par_value,
        shares_outstanding = s.shares_outstanding,
        is_active = s.is_active,
        updated_ts = s.updated_ts
      WHEN NOT MATCHED THEN INSERT *
    """)


def fetch_rows_from_kis(symbols: list[str], *, kis_client) -> list[Row]:
    """KIS REST search-stock-info (CTPF1604R) 로 종목 메타 조회.

    이 TR 은 종목명 / 상품명 / 영문명만 반환 — 시장/액면가/발행주식수는
    별도 TR 필요. Phase 1 은 종목명 표시 목적이라 prdt_abrv_name 만 활용,
    market 은 KOSPI 하드코딩, par_value/shares_outstanding 은 0 유지.
    Phase 1B 에서 추가 TR (예: inquire-price) 로 보강.
    """
    now = datetime.now(KST).replace(tzinfo=None)  # KST 벽시계 (다른 pipeline 과 일치)
    out: list[Row] = []
    for sym in symbols:
        info = kis_client.get_stock_info(sym)
        out.append(Row(
            symbol=sym,
            symbol_name=info.get("prdt_abrv_name") or info.get("prdt_name") or sym,
            market="KOSPI",
            par_value=Decimal("0"),
            shares_outstanding=0,
            is_active=True,
            updated_ts=now,
        ))
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default=os.environ.get("KIS_SYMBOLS", "005930,000660,035420"))
    p.add_argument("--dim-table", default="glue.tickberg.silver_dim_symbol")
    args = p.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    # spark-submit 실행 시 dim_symbol_daily.py 의 디렉터리 (code/pipelines/silver) 가
    # sys.path 에 포함되어 sibling module 로 import. pytest 에선 main() 호출 안 하므로
    # 이 import 가 실행되지 않아 test 영향 없음.
    from kis_rest import KisRestClient

    client = KisRestClient.from_env()
    rows = fetch_rows_from_kis(symbols, kis_client=client)

    spark = SparkSession.builder.appName("dim_symbol_daily").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    df = spark.createDataFrame(rows)
    merge_dim_symbol(spark, src_df=df, dim_table=args.dim_table)
    print(f"dim_symbol merged rows={len(rows)}")


if __name__ == "__main__":
    main()
