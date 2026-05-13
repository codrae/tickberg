CREATE TABLE IF NOT EXISTS tickberg.silver_kis_tick_clean (
  trade_uid       string,
  symbol          string,
  trade_ts_kst    timestamp,
  trade_ts_utc    timestamp,
  price           decimal(18,2),
  volume          bigint,
  trade_amount    decimal(20,2),
  trade_side      string,
  best_ask_price  decimal(18,2),
  best_bid_price  decimal(18,2),
  ingest_ts       timestamp,
  silver_ts       timestamp
)
PARTITIONED BY (hour(trade_ts_kst))
LOCATION 's3://tickberg-lakehouse/silver/kis_tick_clean/'
TBLPROPERTIES (
  'table_type'                        = 'ICEBERG',
  'format'                            = 'parquet',
  'write_target_data_file_size_bytes' = '268435456'
);
