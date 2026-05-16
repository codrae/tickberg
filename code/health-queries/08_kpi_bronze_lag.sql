-- KPI 1: Bronze 적재 lag (목표: p95 < 2분)
-- 측정 윈도우: 2026-05-11 KST 10:00–12:59 (안정 운영 구간)
-- 09:00–09:59 = streaming warm-up backlog 흡수 구간, 윈도우 밖
-- 13:00 이후 = ingest 중단되어 윈도우 밖
--
-- 함정 메모: trade_ts_kst raw 값은 UTC (Spark session.timeZone=Asia/Seoul 환경에서
-- producer KST string → internal UTC micros 저장). +9h 후 hour 추출해야 KST.
-- lag = ingest_ts(UTC) − trade_ts_kst(UTC) 단순 차이.
--
-- 예상: tick ≈ 178k, p50 ≈ 45s, avg ≈ 50s, p95 ≈ 88s, max ≈ 207s
-- 예상 status: PASS (p95 < 2분, max spike 허용)
WITH ticks AS (
  SELECT
    CAST(date_diff('second', trade_ts_kst, ingest_ts) AS double) AS lag_sec
  FROM tickberg.bronze_kis_tick_raw
  WHERE dt = date '2026-05-11'
    AND hour(trade_ts_kst + interval '9' hour) BETWEEN 10 AND 12
)
SELECT
  date '2026-05-11'                              AS session_date_kst,
  'KST 10:00–12:59'                              AS window_kst,
  count(*)                                       AS tick_count,
  round(min(lag_sec), 2)                         AS min_lag_sec,
  round(approx_percentile(lag_sec, 0.50), 2)     AS p50_lag_sec,
  round(avg(lag_sec), 2)                         AS avg_lag_sec,
  round(approx_percentile(lag_sec, 0.95), 2)     AS p95_lag_sec,
  round(max(lag_sec), 2)                         AS max_lag_sec,
  CASE
    WHEN count(*) = 0                            THEN 'N/A (no data)'
    WHEN max(lag_sec) < 120                      THEN 'PASS (max < 2분)'
    WHEN approx_percentile(lag_sec, 0.95) < 120  THEN 'PASS (p95 < 2분, max spike 허용)'
    ELSE 'FAIL'
  END                                            AS kpi_status
FROM ticks;
