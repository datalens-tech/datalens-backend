#!/bin/bash
set -e

CERT_DIR=/opt/starrocks/certs
KEYSTORE_PASSWORD=changeit
mkdir -p "$CERT_DIR"

curl -f "$CERT_SERVER_URL/ca.pem" -o "$CERT_DIR/ca.pem"
curl -f "$CERT_SERVER_URL/server-cert.pem" -o "$CERT_DIR/server-cert.pem"
curl -f "$CERT_SERVER_URL/server-key.pem" -o "$CERT_DIR/server-key.pem"

openssl pkcs12 -export \
    -in "$CERT_DIR/server-cert.pem" \
    -inkey "$CERT_DIR/server-key.pem" \
    -out "$CERT_DIR/keystore.p12" \
    -name starrocks \
    -password "pass:$KEYSTORE_PASSWORD"

FE_CONF="/data/deploy/starrocks/fe/conf/fe.conf"
echo "" >> "$FE_CONF"
echo "ssl_keystore_location = $CERT_DIR/keystore.p12" >> "$FE_CONF"
echo "ssl_keystore_password = $KEYSTORE_PASSWORD" >> "$FE_CONF"
echo "ssl_key_password = $KEYSTORE_PASSWORD" >> "$FE_CONF"

# Relax StarRocks disk watermarks for CI. The defaults reserve 100GB of free space
# (storage_*_reserve_bytes); CI runners don't have that, so the BE is flagged
# "not enough disk space" and CREATE TABLE fails with "No alive nodes".
BE_CONF="/data/deploy/starrocks/be/conf/be.conf"
{
    echo ""
    echo "storage_flood_stage_left_capacity_bytes=2147483648"
    echo "storage_flood_stage_usage_percent=99"
} >> "$BE_CONF"
{
    echo ""
    echo "storage_usage_hard_limit_reserve_bytes=2147483648"
    echo "storage_usage_hard_limit_percent=99"
} >> "$FE_CONF"

exec /data/deploy/entrypoint.sh
