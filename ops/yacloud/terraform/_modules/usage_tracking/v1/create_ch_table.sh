# event_date - MATERIALIZED toDate(event_time)
# source_entry_id - MATERIALIZED ifNull(dataset_id, ifNull(connection_id, '__unknown__'))
# is_public - DEFAULT toUInt8(empty(user_id))

query=$(cat <<EOF
CREATE TABLE
$CH_DATABASE.$CH_TABLE
ON CLUSTER $CH_CLUSTER (
    event_time DateTime,
    event_date Date,
    source_entry_id String,
    dash_id Nullable(String),
    dash_tab_id Nullable(String),
    chart_id Nullable(String),
    chart_kind Nullable(String),
    response_status_code Nullable(UInt64),
    dataset_id Nullable(String),
    user_id Nullable(String),
    request_id Nullable(String),
    folder_id Nullable(String),
    query Nullable(String),
    source Nullable(String),
    connection_id Nullable(String),
    dataset_mode Nullable(String),
    username Nullable(String),
    execution_time Int64,
    status Nullable(String),
    error Nullable(String),
    connection_type Nullable(String),
    host Nullable(String),
    cluster Nullable(String),
    clique_alias Nullable(String),
    cache_used UInt8,
    cache_full_hit UInt8,
    is_public UInt8,
    endpoint_code Nullable(String),
    query_type Nullable(String),
    err_code Nullable(String),
    _timestamp DateTime,
    _partition String,
    _offset UInt64,
    _idx UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/$CH_TABLE', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (source_entry_id, event_time, event_date)
TTL event_date + INTERVAL 6 MONTH;
EOF
)

echo $query;
echo $CA_CERT_PATH;

response=$(echo $query | curl \
      -i \
      -cacert $CA_CERT_PATH \
      -H "X-ClickHouse-User: $CH_USER" \
      -H "X-ClickHouse-Key: $CH_PASSWORD" \
      "https://$CH_HOST:8443/" \
      --data-binary @-
)

echo $response | head -1 | grep ' 200' || { echo "Can't create CH table\n$response" ; exit 1; }
