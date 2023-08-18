create table
if not exists
  ext_testing.bi_logs
-- on cluster '{cluster}'
(
  -- syslog fields
  PROGRAM Nullable(String),
  PRIORITY Nullable(String),
  MESSAGE String,
  ISODATE String,
  DATE Nullable(String),
  HOST Nullable(String),
  FACILITY Nullable(String),
  -- primary extracted fields (for sorting, partitioning, ...)
  ts DateTime('UTC') MATERIALIZED toDateTime(substring(ISODATE, 1, 19)),
  timestampns Int64 MATERIALIZED ifNull(JSONExtractInt(MESSAGE, 'timestampns'), toInt64(ts) * 1000000000),
  msg Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'message'),
  request_id String MATERIALIZED ifNull(JSONExtractString(MESSAGE, 'request_id'), ''),
  -- other known log fields
  app_instance Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'app_name'),
  app_name Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'app_name'),
  app_version Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'app_version'),
  funcName Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'funcName'),
  hostname Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'hostname'),
  levelname Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'levelname'),
  lineno Nullable(Int64) MATERIALIZED JSONExtractInt(MESSAGE, 'lineno'),
  name Nullable(String) MATERIALIZED JSONExtractString(MESSAGE, 'name'),
  pid Nullable(Int64) MATERIALIZED JSONExtractInt(MESSAGE, 'pid')
)
-- ENGINE = ReplicatedMergeTree('/clickhouse/tables/01-{shard}/ext_testing__bi_logs', '{replica}')
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (request_id, timestampns)
TTL ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192

-- for env in int_testing ext_testing int_prod ext_prod; do _dl_ch_logs_"$env" "drop table if exists $env.bi_logs" '' -v && _dl_ch_logs_"$env" "$(cat ch_schema.sql | sed -r "s/ext_testing/${env}/g")" '' -v; done
