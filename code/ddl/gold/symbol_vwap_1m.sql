CREATE TABLE IF NOT EXISTS tickberg.gold_symbol_vwap_1m (
  symbol         string,
  ts_minute      timestamp,
  open_price     decimal(18,2),
  close_price    decimal(18,2),
  high_price     decimal(18,2),
  low_price      decimal(18,2),
  total_volume   bigint,
  vwap           decimal(18,4),
  trade_count    int,
  computed_at    timestamp
)
PARTITIONED BY (hour(ts_minute))
LOCATION 's3://tickberg-lakehouse/gold/symbol_vwap_1m/'
TBLPROPERTIES (
  'table_type'                        = 'ICEBERG',
  'format'                            = 'parquet',
  'write_target_data_file_size_bytes' = '268435456'
);
