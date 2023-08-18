#!/bin/bash -x

set -Eeu

until [[ "$(echo SELECT 1 FROM DUAL | sqlplus sys/Oradoc_db1@db-oracle/ORCLPDB1.localdomain as sysdba | grep -c ERROR)" == 0 ]]
do
    echo 'Waiting for Oracle DB to initialize...'
    sleep 15
done
echo "Oracle Probably Connected."

sqlplus sys/Oradoc_db1@db-oracle/ORCLPDB1.localdomain as sysdba < /oracle/docker-entrypoint-initdb.d/initialize.sql
