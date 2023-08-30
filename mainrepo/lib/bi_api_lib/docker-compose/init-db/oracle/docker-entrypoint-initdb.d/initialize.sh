#! /bin/bash


echo 'Waiting for Oracle DB to initialize...'
until [[ `echo SELECT 1 FROM DUAL | sqlplus sys/Oradoc_db1@db-oracle/ORCLPDB1.localdomain as sysdba | grep -c ERROR` == 0 ]]
do
    sleep 15
done
echo Connected

sqlplus sys/Oradoc_db1@db-oracle/ORCLPDB1.localdomain as sysdba < /oracle/docker-entrypoint-initdb.d/initialize.sql
