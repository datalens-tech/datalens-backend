#!/bin/sh -x

set -Eeu

until /opt/mssql-tools/bin/sqlcmd -S db-mssql -U sa -P qweRTY123 -Q "SELECT 1" > /dev/null
do
    echo 'Waiting for MSSQL DB to initialize...'
    sleep 15
done
echo "MSSQL Connected."

for db_name in common_test datalens_test; do
    /opt/mssql-tools/bin/sqlcmd -S db-mssql -U sa -P qweRTY123 -Q "sp_configure 'contained database authentication', 1; RECONFIGURE; IF db_id(N'${db_name}') IS NULL CREATE DATABASE ${db_name} CONTAINMENT = PARTIAL"
    /opt/mssql-tools/bin/sqlcmd -S db-mssql -U sa -P qweRTY123 -d "${db_name}" -Q "IF NOT EXISTS(SELECT * FROM sys.database_principals where name = 'datalens') CREATE USER datalens WITH PASSWORD = 'qweRTY123'"
    /opt/mssql-tools/bin/sqlcmd -S db-mssql -U sa -P qweRTY123 -d "${db_name}" -Q "GRANT CREATE TABLE, CREATE VIEW, ALTER, INSERT, SELECT, UPDATE, DELETE TO datalens"
done
