-- 영업시간 hour 별 분봉 row 수 → 180 = 3 종목 × 60 분 (정상)
SELECT
  date_trunc('hour', ts_minute) AS hr,
  count(*) AS row_count,
  count(distinct symbol) AS symbol_count,
  180 AS expected_rows
FROM tickberg.gold_symbol_vwap_1m
WHERE ts_minute >= current_date AND ts_minute < current_date + interval '1' day
GROUP BY 1
ORDER BY 1;
