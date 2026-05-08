-- 최근 15분 종목별 tick count. 3 종목 모두 보여야 정상.
SELECT
  symbol,
  count(*) AS tick_count,
  max(trade_ts_kst) AS last_trade,
  count(distinct date_trunc('minute', trade_ts_kst)) AS active_minutes
FROM tickberg.silver_kis_tick_clean
WHERE silver_ts >= current_timestamp - interval '15' minute
GROUP BY symbol
ORDER BY symbol;
