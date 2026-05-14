CREATE TABLE IF NOT EXISTS tickberg.silver_dart_disclosure_clean (
  rcept_no     string,
  stock_code   string,
  corp_name    string,
  report_nm    string,
  report_kind  string,
  rcept_date   date,
  flr_nm       string,
  silver_ts    timestamp
)
PARTITIONED BY (rcept_date)
LOCATION 's3://tickberg-lakehouse/silver/dart_disclosure_clean/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format'     = 'parquet'
);
