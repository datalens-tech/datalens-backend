#!/bin/bash
set -e

# Fetch certificates from the SSL provider
CERT_SERVER_URL=${CERT_SERVER_URL:-http://ssl-provider}

echo "Fetching certificates from $CERT_SERVER_URL..."
curl -f $CERT_SERVER_URL/ca.pem -o /etc/mysql/certs/ca.pem
curl -f $CERT_SERVER_URL/server-cert.pem -o /etc/mysql/certs/server-cert.pem
curl -f $CERT_SERVER_URL/server-key.pem -o /etc/mysql/certs/server-key.pem

echo "Certificates fetched successfully!"

# Start MySQL with SSL enabled
exec docker-entrypoint.sh "$@" \
    --require_secure_transport=ON \
    --ssl-ca=/etc/mysql/certs/ca.pem \
    --ssl-cert=/etc/mysql/certs/server-cert.pem \
    --ssl-key=/etc/mysql/certs/server-key.pem
