CREATE EXTERNAL TABLE IF NOT EXISTS tickberg.bronze_dart_disclosure_raw (
  rcept_no    string,
  corp_code   string,
  corp_name   string,
  stock_code  string,
  report_nm   string,
  rcept_dt    string,
  flr_nm      string,
  rm          string,
  ingest_ts   timestamp
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/dart_disclosure_raw/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.dt.type'         = 'date',
  'projection.dt.range'        = '2026-05-01,NOW',
  'projection.dt.format'       = 'yyyy-MM-dd',
  'projection.dt.interval'     = '1',
  'projection.dt.interval.unit'= 'DAYS',
  'storage.location.template'  = 's3://tickberg-lakehouse/bronze/dart_disclosure_raw/dt=${dt}/'
);
