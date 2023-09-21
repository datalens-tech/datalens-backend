from __future__ import annotations

import contextlib
from typing import Tuple

import pytest

import bi_legacy_test_bundle_tests.api_lib.config as tests_config_mod
from bi_legacy_test_bundle_tests.api_lib.conftest import (  # noqa
    DB_PARAMS,
    _db_for,
    ch_connection_params,
    create_ch_connection,
    db_for_key,
    loaded_libraries,
)
from bi_legacy_test_bundle_tests.api_lib.utils import (
    get_random_str,
    make_connection_get_id,
)
from dl_api_lib.enums import USPermissionKind
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_constants.enums import BIType
from dl_core.united_storage_client import UStorageClientBase
from dl_core_testing.database import (
    C,
    CoreReInitableDbDispenser,
    Db,
    DbTable,
    make_table,
)
from dl_core_testing.environment import (
    common_pytest_configure,
    prepare_united_storage_from_config,
)

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE


def pytest_configure(config):  # noqa
    bi_test_config = tests_config_mod.BI_TEST_CONFIG
    common_pytest_configure()  # make sure the CH dialect is available.
    prepare_united_storage_from_config(us_config=bi_test_config.core_test_config.get_us_config())


@pytest.fixture(scope="function", autouse=True)
def reinit_dbs(db_dispenser) -> None:
    assert isinstance(db_dispenser, CoreReInitableDbDispenser)
    db_dispenser.check_reinit_all()


@pytest.fixture(scope="session")
def mssql_db(request, initdb_ready, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_MSSQL, db_dispenser=db_dispenser)


@pytest.fixture(scope="session")
def mysql_db(request, initdb_ready, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_MYSQL, db_dispenser=db_dispenser)


@pytest.fixture(scope="session")
def oracle_db(request, initdb_ready, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_ORACLE, db_dispenser=db_dispenser)


@pytest.fixture(scope="session")
def postgres_db(request, initdb_ready, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_POSTGRES, db_dispenser=db_dispenser)


@pytest.fixture(scope="session")
def postgres_db_fresh(request, initdb_ready, db_dispenser) -> Db:
    return db_for_key("postgres_fresh", conn_type=CONNECTION_TYPE_POSTGRES, db_dispenser=db_dispenser)


@pytest.fixture(scope="session")
def two_clickhouse_tables(clickhouse_db) -> Tuple[DbTable, DbTable]:
    """Two basic tables for ClickHouse"""
    return make_table(clickhouse_db), make_table(clickhouse_db)


@pytest.fixture(scope="session")
def two_clickhouse_tables_w_const_columns(clickhouse_db) -> Tuple[DbTable, DbTable]:
    """Two basic table for ClickHouse"""
    columns = [
        C(name="id", user_type=BIType.integer, vg=lambda rn, **kwargs: rn),
        C(name="const", user_type=BIType.integer, vg=lambda rn, **kwargs: 8),
    ]
    return make_table(clickhouse_db, columns=columns), make_table(clickhouse_db, columns=columns)


@pytest.fixture(scope="session")
def three_clickhouse_tables(clickhouse_db) -> Tuple[DbTable, DbTable, DbTable]:
    """Two basic tables for ClickHouse"""
    return make_table(clickhouse_db), make_table(clickhouse_db), make_table(clickhouse_db)


@pytest.fixture(scope="session")
def clickhouse_table(clickhouse_db) -> DbTable:
    """Basic table for ClickHouse"""
    return make_table(clickhouse_db)


@pytest.fixture(scope="session")
def postgres_table(postgres_db) -> DbTable:
    """Basic table for PostgreSQL"""
    return make_table(postgres_db)


@pytest.fixture(scope="session")
def postgres_table_fresh(postgres_db_fresh) -> DbTable:
    """Basic table for PostgreSQL"""
    return make_table(postgres_db_fresh)


@pytest.fixture(scope="session")
def oracle_table(oracle_db) -> DbTable:
    """Basic table for Oracle"""
    return make_table(oracle_db)


@pytest.fixture(scope="function")
def connection_id(app, client, request):  # TODO: Rename to ch_connection_id
    return create_ch_connection(app, client, request)


@pytest.fixture(scope="function")
def ch_data_source_settings(static_connection_id):
    return {
        "connection_id": static_connection_id,
        "source_type": "CH_TABLE",
        "parameters": {
            "db_name": "test_data",
            "table_name": "sample_superstore",
        },
    }


@pytest.fixture(scope="function")
def ch_data_source_settings_for_other_db(static_connection_id):
    return {
        "connection_id": static_connection_id,
        "source_type": "CH_TABLE",
        "parameters": {
            "db_name": "other_test_data",
            "table_name": "sample_superstore",
        },
    }


@pytest.fixture(scope="function")
def ch_other_data_source_settings(ch_data_source_settings):
    return dict(
        ch_data_source_settings,
        parameters=dict(
            ch_data_source_settings["parameters"],
            table_name="SampleLite",
        ),
    )


@pytest.fixture()
def pg_connection_params():
    db_params = DB_PARAMS["pg"]
    return {
        "name": "pg_test_{}".format(get_random_str()),
        "type": "postgres",
        "host": db_params.host.split(":")[0],
        "port": int(db_params.host.split(":")[1]),
        "db_name": "datalens",
        "username": "datalens",
        "password": db_params.password,
    }


@pytest.fixture()
def pg_fresh_connection_params():
    db_params = DB_PARAMS["pg_fresh"]
    return {
        "name": "pg_test_{}".format(get_random_str()),
        "type": "postgres",
        "host": db_params.host.split(":")[0],
        "port": int(db_params.host.split(":")[1]),
        "db_name": "datalens",
        "username": "datalens",
        "password": db_params.password,
    }


@pytest.fixture(scope="function")
def pg_connection_id(app, client, request, pg_connection_params):
    return make_connection_get_id(connection_params=pg_connection_params, client=client, request=request)


@pytest.fixture(scope="function")
def pg_fresh_connection_id(app, client, request, pg_fresh_connection_params):
    return make_connection_get_id(connection_params=pg_fresh_connection_params, client=client, request=request)


@pytest.fixture(scope="function")
def pg_subselectable_connection_id(app, client, request, pg_connection_params):
    return make_connection_get_id(
        connection_params=dict(
            pg_connection_params,
            raw_sql_level="dashsql",
        ),
        client=client,
        request=request,
    )


@pytest.fixture(scope="function")
def mysql_connection_id(app, client, request):
    db_params = DB_PARAMS["mysql"]
    conn_params = {
        "name": "mysql_test_{}".format(get_random_str()),
        "type": "mysql",
        "host": db_params.host.split(":")[0],
        "port": int(db_params.host.split(":")[1]),
        "db_name": "partner",
        "username": "datalens",
        "password": db_params.password,
        "raw_sql_level": "dashsql",  # NOTE: allowing by default
    }
    return make_connection_get_id(connection_params=conn_params, client=client, request=request)


@pytest.fixture(scope="function")
def invalid_mssql_connection_id(app, client, request):
    conn_params = {
        "name": "mssql_test_invalid_{}".format(get_random_str()),
        "type": "mssql",
        "host": "some.definitely.invalid.host.com.au",
        "port": 8989,
        "db_name": "db_name",
        "username": "username",
        "password": "password",
    }
    return make_connection_get_id(connection_params=conn_params, client=client, request=request)


@pytest.fixture(scope="function")
def mssql_connection_id(app, client, request):
    db_params = DB_PARAMS["mssql"]
    conn_params = {
        "name": "mssql_test_{}".format(get_random_str()),
        "type": "mssql",
        "host": db_params.host.split(":")[0],
        "port": int(db_params.host.split(":")[1]),
        "db_name": "datalens_test",
        "username": "datalens",
        "password": db_params.password,
        "raw_sql_level": "dashsql",  # NOTE: allowing by default
    }
    return make_connection_get_id(connection_params=conn_params, client=client, request=request)


@pytest.fixture(scope="function")
def oracle_connection_id(app, client, request):
    db_params = DB_PARAMS["oracle"]

    # tmp workaround until we moved away from arcadia
    import sys

    conn_params = {
        "name": "oracle_test_{}".format(get_random_str()),
        "type": "oracle",
        "host": db_params.host.split(":")[0],
        "port": int(db_params.host.split(":")[1]),
        "db_connect_method": "service_name",
        "db_name": "XEPDB1",
        "username": "datalens",
        "password": db_params.password,
        "raw_sql_level": "dashsql",  # NOTE: allowing by default
    }
    return make_connection_get_id(connection_params=conn_params, client=client, request=request)


@pytest.fixture
def mock_permissions_for_us_entries(monkeypatch):
    @contextlib.contextmanager
    def inner(**permissions: dict[USPermissionKind, bool]):
        unmocked = UStorageClientBase._get_us_json_from_response

        def mocked(client: UStorageClientBase, response: UStorageClientBase.ResponseAdapter):
            json = unmocked(response)
            json["permissions"].update(permissions)
            return json

        monkeypatch.setattr(UStorageClientBase, "_get_us_json_from_response", mocked)
        try:
            yield
        finally:
            monkeypatch.undo()

    return inner
