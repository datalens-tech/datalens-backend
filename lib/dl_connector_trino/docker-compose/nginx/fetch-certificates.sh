#!/bin/sh
set -e

# Fetch certificates from the SSL provider
CERT_SERVER_URL=${CERT_SERVER_URL:-http://ssl-provider}

echo "Fetching certificates from $CERT_SERVER_URL..."
curl -f $CERT_SERVER_URL/ca.pem -o /etc/nginx/certs/ca.pem
curl -f $CERT_SERVER_URL/server-cert.pem -o /etc/nginx/certs/server-cert.pem
curl -f $CERT_SERVER_URL/server-key.pem -o /etc/nginx/certs/server-key.pem

echo "Certificates fetched successfully!"
