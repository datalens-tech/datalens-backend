#! /bin/bash

echo 'Waiting for MSSQL DB to initialize...'
until /opt/mssql-tools/bin/sqlcmd -S db-mssql-14 -U sa -P qweRTY123 -Q "SELECT 1" > /dev/null
do
    sleep 15
done
echo Connected

/opt/mssql-tools/bin/sqlcmd -S db-mssql-14 -U sa -P qweRTY123 -Q "sp_configure 'contained database authentication', 1; RECONFIGURE; IF db_id(N'test_data') IS NULL CREATE DATABASE test_data CONTAINMENT = PARTIAL"
/opt/mssql-tools/bin/sqlcmd -S db-mssql-14 -U sa -P qweRTY123 -d test_data -Q "IF NOT EXISTS(SELECT * FROM sys.database_principals where name = 'datalens') CREATE USER datalens WITH PASSWORD = 'qweRTY123'"
/opt/mssql-tools/bin/sqlcmd -S db-mssql-14 -U sa -P qweRTY123 -d test_data -Q "GRANT CREATE TABLE, ALTER, INSERT, SELECT, UPDATE, DELETE TO datalens"
/opt/mssql-tools/bin/sqlcmd -S db-mssql-14 -U sa -P qweRTY123 -d test_data -i /mssql/docker-entrypoint-initdb.d/prepare_db.sql
