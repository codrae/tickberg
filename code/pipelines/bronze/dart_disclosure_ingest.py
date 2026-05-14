"""Daily DART disclosure → Bronze Parquet (06:00 KST).

전 영업일 (default = yesterday KST) 의 DART 공시 list 를 조회 후 우리 종목 (KIS_SYMBOLS)
만 필터링하여 s3://.../bronze/dart_disclosure_raw/dt=YYYY-MM-DD/ 파티션에 append.

DART_API_KEY 는 spark-master 컨테이너의 env (env_file: .env) 에서 직접 읽음 — Airflow
Variables 불필요.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import Row

# sibling module — spark-submit 시 script directory 가 sys.path 에 포함됨.
from dart_client import DartClient

KST = ZoneInfo("Asia/Seoul")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--business-date", default=None, help="YYYYMMDD; default = yesterday KST")
    p.add_argument("--symbols", default=os.environ.get("KIS_SYMBOLS", "005930,000660,035420"))
    args = p.parse_args()

    biz = args.business_date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    client = DartClient(api_key=os.environ["DART_API_KEY"])
    rows = client.fetch_disclosures(business_date=biz, stock_codes=symbols)

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from _spark import build_batch_session

    spark = build_batch_session("dart_disclosure_ingest")

    if not rows:
        print(f"no DART rows for {biz}")
        return

    now = datetime.now(KST).replace(tzinfo=None)
    dt = datetime.strptime(biz, "%Y%m%d").date()
    spark_rows = [
        Row(
            rcept_no=r.get("rcept_no", ""), corp_code=r.get("corp_code", ""),
            corp_name=r.get("corp_name", ""), stock_code=r.get("stock_code", ""),
            report_nm=r.get("report_nm", ""), rcept_dt=r.get("rcept_dt", ""),
            flr_nm=r.get("flr_nm", ""), rm=r.get("rm", ""),
            ingest_ts=now, dt=dt,
        )
        for r in rows
    ]
    df = spark.createDataFrame(spark_rows)
    bucket = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
    (df.write.mode("append").format("parquet")
       .partitionBy("dt")
       .save(f"s3a://{bucket}/bronze/dart_disclosure_raw/"))
    print(f"wrote {len(spark_rows)} DART rows for {biz}")


if __name__ == "__main__":
    main()
