#!/bin/bash
set -e

CERT_SERVER_URL=${CERT_SERVER_URL:-http://ssl-provider}

echo "Fetching certificates from $CERT_SERVER_URL..."
curl -f "$CERT_SERVER_URL/ca.pem" -o /etc/clickhouse-server/certs/marsnet_ca.crt
curl -f "$CERT_SERVER_URL/server-cert.pem" -o /etc/clickhouse-server/certs/chnode.crt
curl -f "$CERT_SERVER_URL/server-key.pem" -o /etc/clickhouse-server/certs/chnode.key

chown clickhouse:clickhouse /etc/clickhouse-server/certs/*
chmod 600 /etc/clickhouse-server/certs/*

echo "Certificates fetched successfully!"

exec /entrypoint.sh "$@"
