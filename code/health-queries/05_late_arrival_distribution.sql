-- Late arrival 분포 — "trade_ts_kst → silver_ts 까지 몇 초 걸렸나" 5 버킷.
-- 영업시간 정상 = 대부분 <1s, 일부 1–10s. >60s 면 streaming/MERGE 지연 의심.
-- T4 ad-hoc 디버깅용 (Athena workgroup tickberg-wg scan ≤ 5GB).
WITH lagged AS (
  SELECT date_diff('second', trade_ts_kst, silver_ts) AS lag_s
  FROM tickberg.silver_kis_tick_clean
  WHERE silver_ts >= current_timestamp - INTERVAL '1' DAY
)
SELECT
  CASE
    WHEN lag_s < 1    THEN '01_under_1s'
    WHEN lag_s < 10   THEN '02_1_to_10s'
    WHEN lag_s < 60   THEN '03_10_to_60s'
    WHEN lag_s < 600  THEN '04_60s_to_10m'
    ELSE                   '05_over_10m'
  END AS bucket,
  count(*) AS row_count
FROM lagged
GROUP BY 1
ORDER BY 1;
