-- KPI 2: Silver dedup rate (목표 0.95–1.0)
-- 측정 윈도우: 2026-05-11 KST 10:00–12:59 (KPI 1 과 동일 윈도우)
-- bronze 행 수 vs silver 행 수. 1.0 = 중복 0건, 0.95–1.0 = WebSocket 재전송분이
-- trade_uid 기준 MERGE INTO 로 정상 흡수.
--
-- 함정 메모: bronze/silver 의 trade_ts_kst raw 값은 모두 UTC (+9h 후 KST hour).
--
-- 예상: bronze=silver=178,100, dedup_ratio=1.0
-- 예상 status: PASS (0.95–1.0)
-- 참고: 이 윈도우는 dedup_removed=0 → "MERGE 멱등성 유지" narrative
--       (재전송 흡수 효과 시연은 warm-up 구간 등 다른 윈도우 추가 캡쳐로 보완)
WITH bronze AS (
  SELECT count(*) AS c
  FROM tickberg.bronze_kis_tick_raw
  WHERE dt = date '2026-05-11'
    AND hour(trade_ts_kst + interval '9' hour) BETWEEN 10 AND 12
),
silver AS (
  SELECT count(*) AS c
  FROM tickberg.silver_kis_tick_clean
  WHERE CAST(trade_ts_kst AS date) = date '2026-05-11'
    AND hour(trade_ts_kst + interval '9' hour) BETWEEN 10 AND 12
)
SELECT
  date '2026-05-11'                                        AS session_date_kst,
  'KST 10:00–12:59'                                        AS window_kst,
  bronze.c                                                 AS bronze_rows,
  silver.c                                                 AS silver_rows,
  bronze.c - silver.c                                      AS dedup_removed,
  round(CAST(silver.c AS double) / NULLIF(bronze.c, 0), 4) AS dedup_ratio,
  CASE
    WHEN bronze.c = 0                                              THEN 'N/A (no data)'
    WHEN CAST(silver.c AS double) / bronze.c BETWEEN 0.95 AND 1.0  THEN 'PASS (0.95–1.0)'
    ELSE 'FAIL'
  END                                                      AS kpi_status
FROM bronze, silver;
