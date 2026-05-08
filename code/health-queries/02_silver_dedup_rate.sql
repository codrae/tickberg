-- Bronze count vs Silver count 1h. 정상 0.95–1.0 (Silver=Bronze 또는 약간 적음).
WITH bronze_h AS (
  SELECT count(*) AS c FROM tickberg.bronze_kis_tick_raw
  WHERE ingest_ts >= current_timestamp - interval '1' hour
), silver_h AS (
  SELECT count(*) AS c FROM tickberg.silver_kis_tick_clean
  WHERE silver_ts >= current_timestamp - interval '1' hour
)
SELECT
  bronze_h.c AS bronze_rows,
  silver_h.c AS silver_rows,
  CAST(silver_h.c AS double) / NULLIF(bronze_h.c, 0) AS dedup_ratio
FROM bronze_h, silver_h;
