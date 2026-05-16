-- KPI 3: Gold 1분봉 결손 (목표 = 0)
-- 측정 윈도우: 2026-05-14 KST 10:00–12:59
-- (5/11 은 gold 미적재 → KPI 3 만 5/14 별도. KPI 1/2 와 동일 시간대로 일관성 유지)
--
-- 함정 메모: 5/14 gold ts_minute raw 값은 KST wall-clock (5/8 의 UTC raw 와 패턴 다름)
--           → +9h 보정 없이 hour(ts_minute) 그대로 KST hour. (별도 정리 이슈)
--
-- expected 방식: bronze 에 실제 거래 발생한 (symbol, minute) 조합 수
-- 정규장 모든 분(180×N=540) 이 아니라 거래 발생 분만 — KOSPI 일부 종목은 분 단위
-- 무체결이 자연스럽기 때문. silver→gold 1분봉 생성 결손 0 = 완전 처리.
--
-- 예상: expected_B=357, actual=357, missing=0
-- 예상 status: PASS (결손 0)
WITH symbols AS (
  SELECT count(DISTINCT symbol) AS n
  FROM tickberg.gold_symbol_vwap_1m
  WHERE CAST(ts_minute AS date) = date '2026-05-14'
    AND hour(ts_minute) BETWEEN 10 AND 12
),
gold_rows AS (
  SELECT count(*) AS c
  FROM tickberg.gold_symbol_vwap_1m
  WHERE CAST(ts_minute AS date) = date '2026-05-14'
    AND hour(ts_minute) BETWEEN 10 AND 12
),
bronze_active_min AS (
  SELECT count(*) AS c FROM (
    SELECT DISTINCT
      symbol,
      date_trunc('minute', trade_ts_kst + interval '9' hour) AS minute_kst
    FROM tickberg.bronze_kis_tick_raw
    WHERE dt = date '2026-05-14'
      AND hour(trade_ts_kst + interval '9' hour) BETWEEN 10 AND 12
  ) t
)
SELECT
  'B) bronze 거래 발생 (symbol, minute)'                              AS method,
  (SELECT c FROM bronze_active_min)                                   AS expected_rows,
  (SELECT c FROM gold_rows)                                           AS actual_rows,
  (SELECT c FROM bronze_active_min) - (SELECT c FROM gold_rows)       AS missing_rows,
  CASE WHEN (SELECT c FROM bronze_active_min) - (SELECT c FROM gold_rows) = 0
       THEN 'PASS (결손 0)' ELSE 'FAIL' END                           AS kpi_status
UNION ALL
SELECT
  'A) 정규장 모든 분 (180min × N) — 참고',
  180 * (SELECT n FROM symbols),
  (SELECT c FROM gold_rows),
  180 * (SELECT n FROM symbols) - (SELECT c FROM gold_rows),
  CASE WHEN 180 * (SELECT n FROM symbols) - (SELECT c FROM gold_rows) = 0
       THEN 'PASS' ELSE 'INFO (무거래 분 포함 기준)' END;
