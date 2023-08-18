#! /bin/bash

set -x

rm -rf docker-compose/init-db/oracle
cp -r github/docker-compose/init-db/oracle docker-compose/init-db/

ls -laht docker-compose/init-db/
ls -laht docker-compose/init-db/oracle

