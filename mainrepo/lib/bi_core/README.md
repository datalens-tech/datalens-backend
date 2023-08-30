# bi_core #

Common libraries for BI backend components


## Testing ##

Requirements:

  - Working `docker-compose`
  - `ya` (from arcadia)
  - ssh key in a ssh agent
  - authentication into the docker registry and role `viewer` for repositories:
    - data-ui/united-storage
    - datalens/hhell/dls_p2
    - statinfra/oracle-database-enterprise
  - access to the secrets in the vault:
    - https://yav.yandex-team.ru/secret/sec-01d5pcrfv9xceanxj2jwaed4bm/explore/versions

Run:

    make test

to start tests in docker containers.


## Building and Uploading

Should not be done anymore, because of arcadia tier0


### Dev-env Databases ###

Commands for running SQL in the dev/test environment (docker-compose) databases:


#### clickhouse ####

    curl -v 'http://localhost:50310/' --data-binary 'select 1 format JSON'

    docker exec -it bi_core_db-clickhouse_1 clickhouse-client --query 'select 1 format JSON'


#### postgresql ####

    docker exec -it bi_core_db-postgres_1 psql --username=datalens common_test -c 'select 1;'


#### mysql ####

    docker exec -it bi_core_db-mysql_1 mysql common_test -e 'select 1;'


#### mssql ####

    docker exec -it bi_core_db-mssql_1 /opt/mssql-tools/bin/sqlcmd -S db-mssql -U sa -P qweRTY123 -d common_test -Q "select 1;"


#### oracle ####

    docker exec -it bi_core_db-oracle_1 bash -l -c "printf \"%s\\n\" \"\$0\" | sqlplus sys/Oradoc_db1@db-oracle/ORCLPDB1.localdomain as sysdba" "select 1 from dual;"
