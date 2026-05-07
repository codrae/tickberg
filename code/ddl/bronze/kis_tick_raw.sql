CREATE EXTERNAL TABLE IF NOT EXISTS tickberg.bronze_kis_tick_raw (
  ingest_ts          timestamp,
  kafka_partition    int,
  kafka_offset       bigint,
  symbol             string,
  trade_ts_kst       timestamp,
  price              decimal(18,2),
  volume             bigint,
  cum_volume         bigint,
  cum_amount         bigint,
  trade_side         string,
  best_ask_price     decimal(18,2),
  best_bid_price     decimal(18,2),
  raw_payload        string
)
PARTITIONED BY (dt date, hr int)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/kis_tick_raw/'
TBLPROPERTIES (
  'projection.enabled'                = 'true',
  'projection.dt.type'                = 'date',
  'projection.dt.range'               = '2026-05-01,NOW',
  'projection.dt.format'              = 'yyyy-MM-dd',
  'projection.dt.interval'            = '1',
  'projection.dt.interval.unit'       = 'DAYS',
  'projection.hr.type'                = 'integer',
  'projection.hr.range'               = '0,23',
  'storage.location.template'         = 's3://tickberg-lakehouse/bronze/kis_tick_raw/dt=${dt}/hr=${hr}/'
);
