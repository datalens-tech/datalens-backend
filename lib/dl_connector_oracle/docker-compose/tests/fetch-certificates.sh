#!/bin/bash
set -e

# Fetch certificates from the SSL provider
CERT_SERVER_URL=${CERT_SERVER_URL:-http://ssl-provider}

echo "Fetching certificates from $CERT_SERVER_URL..."
mkdir -p /opt/oracle/tls
curl -f $CERT_SERVER_URL/ca.pem -o /opt/oracle/tls/ca.pem
curl -f $CERT_SERVER_URL/server-cert.pem -o /opt/oracle/tls/cert.pem
curl -f $CERT_SERVER_URL/server-key.pem -o /opt/oracle/tls/key.pem
curl -f $CERT_SERVER_URL/server-key.pk8 -o /opt/oracle/tls/key.pk8

echo "Certificates fetched successfully!"

orapki wallet create -wallet /opt/oracle/wallet -auto_login -pwd Passw0rd
orapki wallet add -wallet /opt/oracle/wallet -trusted_cert -cert /opt/oracle/tls/ca.pem -pwd Passw0rd
orapki wallet add -wallet /opt/oracle/wallet -cert /opt/oracle/tls/cert.pem -pwd Passw0rd

orapki wallet import_private_key -wallet /opt/oracle/wallet -pvtkeyfile /opt/oracle/tls/key.pk8 -cert /opt/oracle/tls/cert.pem -pwd Passw0rd -pvtkeypwd Passw0rd

echo "Certificates imported successfully!"

exec container-entrypoint.sh
