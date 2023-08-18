# set variables
# $CH_USER
# $CH_PASSWORD
# $CH_HOST
# $CH_DATABASE
# $CH_TABLE
# $CH_CLUSTER
# $CA_CERT_PATH

query=$(cat <<EOF
CREATE TABLE
$CH_DATABASE.$CH_TABLE
ON CLUSTER $CH_CLUSTER (
    request_id String,
    datetime DateTime,
    message String,
    app_name String,
    app_version String,
    caller_info String,
    name String,
    hostname String,
    pid UInt64,
    level_name String,
    timestamp UInt64,
    _timestamp DateTime,
    _partition String,
    _offset UInt64,
    _idx UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/{database}/$CH_TABLE', '{replica}')
ORDER BY (request_id, timestamp)
TTL datetime + INTERVAL 3 MONTH;
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
