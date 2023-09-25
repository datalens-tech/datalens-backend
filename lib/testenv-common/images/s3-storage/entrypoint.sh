#!/bin/bash -x

set -e

# Fix server configuration
sed -i 's/s3.amazonaws.com/s3-storage/g' /usr/src/app/config.json

# Run the server
exec npm start
