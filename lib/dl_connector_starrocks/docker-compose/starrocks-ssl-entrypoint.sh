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

exec /data/deploy/entrypoint.sh
