-- KPI 1 보조: Bronze 적재 lag 분당 추이 (시계열 그래프 캡쳐용)
-- 측정 윈도우: 2026-05-11 KST 10:00–12:59
-- trade_ts_kst raw 값은 UTC, +9h 후 KST hour 추출
SELECT
  date_trunc('minute', trade_ts_kst + interval '9' hour)                                            AS minute_kst,
  count(*)                                                                                         AS rows_ingested,
  round(avg(CAST(date_diff('second', trade_ts_kst, ingest_ts) AS double)), 2)                      AS avg_lag_sec,
  round(max(CAST(date_diff('second', trade_ts_kst, ingest_ts) AS double)), 2)                      AS max_lag_sec
FROM tickberg.bronze_kis_tick_raw
WHERE dt = date '2026-05-11'
  AND hour(trade_ts_kst + interval '9' hour) BETWEEN 10 AND 12
GROUP BY 1
ORDER BY 1;
