-- "데이터 N분 전 도착". 영업시간 정상 = 1–2분.
-- 영업시간 외 (장 마감 후) 는 lag 가 커지는게 정상이라 alert 임계값을 시간대 분기 (T1 Grafana)
SELECT
  CAST(date_diff('second', max(ingest_ts), current_timestamp) AS double) / 60 AS lag_minutes,
  max(ingest_ts) AS last_ingest,
  current_timestamp AS now_utc
FROM tickberg.bronze_kis_tick_raw
WHERE dt >= current_date - interval '1' day;
