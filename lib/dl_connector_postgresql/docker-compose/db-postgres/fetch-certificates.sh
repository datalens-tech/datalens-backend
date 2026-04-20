#!/bin/bash
set -e

CERT_SERVER_URL=${CERT_SERVER_URL:-http://ssl-provider}

echo "Fetching certificates from $CERT_SERVER_URL..."
curl -f "$CERT_SERVER_URL/ca.pem" -o /etc/ssl/private/root.crt
curl -f "$CERT_SERVER_URL/server-cert.pem" -o /etc/ssl/private/server.crt
curl -f "$CERT_SERVER_URL/server-key.pem" -o /etc/ssl/private/server.key

chown 70:70 /etc/ssl/private/server.key /etc/ssl/private/server.crt /etc/ssl/private/root.crt
chmod 600 /etc/ssl/private/server.key /etc/ssl/private/server.crt /etc/ssl/private/root.crt

echo "Certificates fetched successfully!"

exec docker-entrypoint.sh "$@"
