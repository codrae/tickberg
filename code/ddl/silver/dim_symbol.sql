CREATE TABLE IF NOT EXISTS tickberg.silver_dim_symbol (
  symbol             string,
  symbol_name        string,
  market             string,
  par_value          decimal(18,2),
  shares_outstanding bigint,
  is_active          boolean,
  updated_ts         timestamp
)
PARTITIONED BY (market)
LOCATION 's3://tickberg-lakehouse/silver/dim_symbol/'
TBLPROPERTIES (
  'table_type'      = 'ICEBERG',
  'format'          = 'parquet',
  'format-version'  = '2'
);
