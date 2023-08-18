#! /bin/bash

set -x

rm -rf docker-compose-core/init-db/oracle
cp -r github/docker-compose-core/init-db/oracle docker-compose-core/init-db/
