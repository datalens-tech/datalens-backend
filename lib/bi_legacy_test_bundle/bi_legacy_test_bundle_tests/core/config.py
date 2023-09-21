"""
Configuration point
(gathering the environment and such)

Common note:
`os.environ.get(key, default)` allows setting empty string, whereas
`os.environ.get(key) or default` doesn't,
so it should depend on whether an empty string is supported.
"""

from __future__ import annotations

import os
from typing import Optional

from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport as _get_test_container_hostport


DOCKER_COMPOSE_FILE_NAME = "docker-compose.core.yml"


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
    COMPOSE_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "bi_legacy_test_bundle")

    MSSQL_CONTAINER_LABEL = "db-mssql"
    ORACLE_CONTAINER_LABEL = "db-oracle"

    US_MASTER_TOKEN = "AC1ofiek8coB"

    DB_CH_CLUSTER = "main"


DOCKER_COMPOSE = DockerCompose()

CONNECTOR_WHITELIST = [
    "testing",
    "clickhouse",
    "postgresql",
    "mysql",
    "mssql",
    "oracle",
    "bigquery",
    "snowflake",
    "ydb",
    "chyt",
    "chyt_internal",
    "ch_frozen_bumpy_roads",
    "ch_frozen_covid",
    "ch_frozen_demo",
    "ch_frozen_dtp",
    "ch_frozen_gkh",
    "ch_frozen_samples",
    "ch_frozen_transparency",
    "ch_frozen_weather",
    "ch_frozen_horeca",
    "file",
    "gsheets_v2",
    "yq",
    "metrica_api",
    "appmetrica_api",
    "ch_billing_analytics",
    "monitoring",
    "solomon",
    "usage_tracking",
    "bitrix_gds",
    "ch_ya_music_podcast_stats",
    "moysklad",
    "equeo",
    "kontur_market",
    "market_couriers",
    "schoolbook",
    "smb_heatmaps",
    "ch_geo_filtered",
]

# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=50300).host,
    port_us_http=get_test_container_hostport("us", fallback_port=50300).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=50309).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=50309).port,
    us_master_token=DOCKER_COMPOSE.US_MASTER_TOKEN,
    core_connector_ep_names=CONNECTOR_WHITELIST,
)

EXT_QUERY_EXECUTER_SECRET_KEY = "_tests_eqe_secret_key_"

SEC_MAPS_GEOCODING_API_TOKEN = os.environ.get("SEC_MAPS_GEOCODING_API_TOKEN") or ""

# Essentially flipping the default to 'on'.
# use `CLEAR_US_DATABASE=` to disable it.
FORCE_CLEAR_US_DATABASE = os.environ.get("CLEAR_US_DATABASE", "1") not in ("0", "")

QUERY_CACHE_REDIS_URL = f'redis://{get_test_container_hostport("redis-caches", fallback_port=50305).as_pair()}/1'
MUTATION_CACHE_REDIS_URL = f'redis://{get_test_container_hostport("redis-caches", fallback_port=50305).as_pair()}/2'

DS_STORE_CH_URL = (
    f"bi_clickhouse://datalens:qwerty@"
    f'{get_test_container_hostport("db-clickhouse", fallback_port=50310).as_pair()}/common_test'
)
DS_STORE_CH_CLUSTER = DOCKER_COMPOSE.DB_CH_CLUSTER
DB_CLICKHOUSE_CLUSTER = DOCKER_COMPOSE.DB_CH_CLUSTER

COMPOSE_PROJECT_NAME = DOCKER_COMPOSE.COMPOSE_PROJECT_NAME
MSSQL_CONTAINER_LABEL = DOCKER_COMPOSE.MSSQL_CONTAINER_LABEL
ORACLE_CONTAINER_LABEL = DOCKER_COMPOSE.ORACLE_CONTAINER_LABEL
ORACLE_PDB = "XEPDB1"


DB_CONFIGURATIONS = dict(
    clickhouse=(DS_STORE_CH_URL, DB_CLICKHOUSE_CLUSTER),
    mysql=(
        f"bi_mysql://datalens:qwerty@"
        f'{get_test_container_hostport("db-mysql", fallback_port=50312).as_pair()}/common_test?charset=utf8',
        None,
    ),
    postgres=(
        f"bi_postgresql://datalens:qwerty@"
        f'{get_test_container_hostport("db-postgres", fallback_port=50313).as_pair()}/common_test',
        None,
    ),
    postgres_fresh=(
        f"bi_postgresql://datalens:qwerty@"
        f'{get_test_container_hostport("db-postgres-fresh", fallback_port=50318).as_pair()}/common_test',
        None,
    ),
    mssql=(
        f"mssql:///?odbc_connect=DRIVER%3D%7BFreeTDS%7D%3B"
        f'Server%3D{get_test_container_hostport("db-mssql", fallback_port=50314).host}'
        f'%3BPort%3D{get_test_container_hostport("db-mssql", fallback_port=50314).port}%3BDatabase%3Dcommon_test%3B'
        f"UID%3Ddatalens%3BPWD%3DqweRTY123%3BTDS_Version%3D8.0",
        None,
    ),
    oracle=(
        f"oracle://datalens:qwerty@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)"
        f'(HOST={get_test_container_hostport("db-oracle", fallback_port=50315).host})'
        f'(PORT={get_test_container_hostport("db-oracle", fallback_port=50315).port}))'
        f"(CONNECT_DATA=(SERVICE_NAME={ORACLE_PDB})))",
        None,
    ),
)
DEFAULT_ORACLE_SCHEMA_NAME = "DATALENS"
OTHER_DBS = dict(
    clickhouse=(
        f"bi_clickhouse://datalens:qwerty@"
        f'{get_test_container_hostport("db-clickhouse", fallback_port=50310).as_pair()}/other_test_db',
        DB_CLICKHOUSE_CLUSTER,
    ),
)
