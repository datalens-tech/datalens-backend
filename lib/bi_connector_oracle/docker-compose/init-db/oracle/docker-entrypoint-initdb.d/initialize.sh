#! /bin/bash

set -x

until [[  $(echo SELECT 1 FROM DUAL | sqlplus -L  datalens/qwerty@//db-oracle/XEPDB1 | grep -c ERROR) == 0 ]]
do
    echo "waiting for oracle startup"
    echo SELECT 1 FROM DUAL | sqlplus -L  datalens/qwerty@//db-oracle/XEPDB1
    sleep 5
done

echo exit | sqlplus -L  sys/qwerty@//db-oracle/XEPDB1 as sysdba @/oracle/docker-entrypoint-initdb.d/prepare_db.sql
