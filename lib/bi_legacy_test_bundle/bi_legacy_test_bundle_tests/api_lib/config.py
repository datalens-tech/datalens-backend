from __future__ import annotations

import os
from typing import Optional

from bi_api_lib_testing_ya.configuration import BiApiTestEnvironmentConfigurationPrivate
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport as _get_test_container_hostport


DOCKER_COMPOSE_FILE_NAME = "docker-compose.api_lib.yml"


def get_test_container_hostport(
    service_key: str,
    original_port: Optional[int] = None,
    fallback_port: Optional[int] = None,
):
    """An evil hack to override the d-c file name"""
    return _get_test_container_hostport(
        service_key=service_key,
        original_port=original_port,
        fallback_port=fallback_port,
        dc_filename=DOCKER_COMPOSE_FILE_NAME,
    )


class DockerCompose:
    REDIS_PASSWORD = "AwockEuvavDyinmeakmiRiopanbesBepsensUrdIz5"

    US_MASTER_TOKEN = "AC1ofiek8coB"
    EXT_QUERY_EXECUTER_SECRET_KEY = "_some_test_secret_key_"

    # ENV key: DB_CLICKHOUSE_CLUSTER
    CH_CLUSTER_NAME = "main"

    COMPOSE_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "bi_api_lib")
    MSSQL_CONTAINER_LABEL = "db-mssql"
    ORACLE_CONTAINER_LABEL = "db-oracle"


DOCKER_COMPOSE = DockerCompose()

COMPOSE_PROJECT_NAME = DOCKER_COMPOSE.COMPOSE_PROJECT_NAME
MSSQL_CONTAINER_LABEL = DOCKER_COMPOSE.MSSQL_CONTAINER_LABEL
ORACLE_CONTAINER_LABEL = DOCKER_COMPOSE.ORACLE_CONTAINER_LABEL

DB_PARAMS = {
    "clickhouse": (
        # ENV key: TEST_DB_CH_HOST
        f'{get_test_container_hostport("db-clickhouse", fallback_port=50510).as_pair()}',
        # ENV key: CH_SAMPLES_PASSWORD_DECRYPT
        "qwerty",
    ),
    "pg": (
        # ENV key: TEST_DB_PG_HOST
        f'{get_test_container_hostport("db-postgres", fallback_port=50513).as_pair()}',
        # ENV key: PG_PASSWORD_DECRYPT
        "qwerty",
    ),
    "pg_fresh": (
        f'{get_test_container_hostport("db-postgres-fresh", fallback_port=50503).as_pair()}',
        "qwerty",
    ),
    "mysql": (
        # ENV key: TEST_DB_MYSQL_HOST
        f'{get_test_container_hostport("db-mysql", fallback_port=50512).as_pair()}',
        # ENV key: MYSQL_PASSWORD_DECRYPT
        "qwerty",
    ),
    "mssql": (
        # ENV key: TEST_DB_MSSQL_HOST
        f'{get_test_container_hostport("db-mssql", fallback_port=50514).as_pair()}',
        # ENV key: MSSQL_PASSWORD_DECRYPT
        "qweRTY123",
    ),
    "external_mssql": (
        # ENV key: TEST_EXTERNAL_DB_MSSQL_HOST
        "130.193.40.16:1433",
        # ENV key: EXTERNAL_MSSQL_PASSWORD_DECRYPT
        None,  # To be filled in fixture via YAV
    ),
    "oracle": (
        # ENV key: TEST_DB_ORACLE_HOST
        f'{get_test_container_hostport("db-oracle", fallback_port=50515).as_pair()}',
        # ENV key: ORACLE_PASSWORD_DECRYPT
        "qwerty",
    ),
    "prometheus": (
        f'{get_test_container_hostport("db-prometheus", fallback_port=50518).as_pair()}',
        "admin",
    ),
}


ORACLE_PDB = "XEPDB1"


DB_URLS = {
    "clickhouse": (
        # ENV key: DB_CLICKHOUSE_URL
        f"bi_clickhouse://test_user:qwerty@"
        f'{get_test_container_hostport("db-clickhouse", fallback_port=50510).as_pair()}/test_data',
        DOCKER_COMPOSE.CH_CLUSTER_NAME,
    ),
    "other_clickhouse": (
        # ENV key: DB_CLICKHOUSE_URL
        f"bi_clickhouse://test_user:qwerty@"
        f'{get_test_container_hostport("db-clickhouse", fallback_port=50510).as_pair()}/other_test_data',
        DOCKER_COMPOSE.CH_CLUSTER_NAME,
    ),
    "mssql": (
        # ENV key: DB_MSSQL_URL
        f"mssql:///?odbc_connect=DRIVER%3D%7BFreeTDS%7D%3B"
        f'Server%3D{get_test_container_hostport("db-mssql", fallback_port=50514).host}%3B'
        f'Port%3D{get_test_container_hostport("db-mssql", fallback_port=50514).port}%3B'
        f"Database%3Ddatalens_test%3B"
        f"UID%3Ddatalens%3BPWD%3DqweRTY123%3BTDS_Version%3D8.0",
        None,
    ),
    "mysql": (
        # ENV key: DB_MYSQL_URL
        f"bi_mysql://datalens:qwerty@"
        f'{get_test_container_hostport("db-mysql", fallback_port=50512).as_pair()}/partner?charset=utf8',
        None,
    ),
    "oracle": (
        # ENV key: DB_ORACLE_URL
        f"oracle://datalens:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)"
        f'(HOST={get_test_container_hostport("db-oracle", fallback_port=50515).host})'
        f'(PORT={get_test_container_hostport("db-oracle", fallback_port=50515).port}))'
        f"(CONNECT_DATA=(SERVICE_NAME={ORACLE_PDB})))",
        None,
    ),
    "postgres": (
        # ENV key: DB_POSTGRES_URL
        f"bi_postgresql://datalens:qwerty@"
        f'{get_test_container_hostport("db-postgres", fallback_port=50513).as_pair()}/datalens',
        None,
    ),
    "postgres_fresh": (
        f"bi_postgresql://datalens:qwerty@"
        f'{get_test_container_hostport("db-postgres-fresh", fallback_port=50503).as_pair()}/datalens',
        None,
    ),
}


BI_TEST_CONFIG = BiApiTestEnvironmentConfigurationPrivate(
    core_test_config=DefaultCoreTestConfiguration(
        host_us_http=get_test_container_hostport("us", fallback_port=50500).host,
        port_us_http=get_test_container_hostport("us", fallback_port=50500).port,
        host_us_pg=get_test_container_hostport("pg-us", fallback_port=50509).host,
        port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=50509).port,
        us_master_token=DOCKER_COMPOSE.US_MASTER_TOKEN,
    ),
    bi_compeng_pg_url=DB_URLS["postgres"][0].replace("bi_postgresql://", "postgresql://"),
    ext_query_executer_secret_key=DOCKER_COMPOSE.EXT_QUERY_EXECUTER_SECRET_KEY,
    redis_host=get_test_container_hostport("redis", fallback_port=50504).host,
    redis_port=get_test_container_hostport("redis", fallback_port=50504).port,  # FIXME: ???
    redis_password=DOCKER_COMPOSE.REDIS_PASSWORD,
    redis_db_default=0,
    redis_db_cache=1,
    redis_db_mutation=2,
    redis_db_arq=11,
)
