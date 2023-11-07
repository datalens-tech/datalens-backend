from frozendict import frozendict

from dl_api_lib_testing.initialization import initialize_api_lib_test
from dl_core_testing.database import (
    CoreDbConfig,
    CoreDbDispenser,
)
from dl_db_testing.database.engine_wrapper import DbEngineConfig
from dl_formula_testing.forced_literal import forced_literal_use

from dl_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from dl_connector_mssql_tests.db.config import (
    ADMIN_URL,
    ADMIN_W_DB_URL,
    API_TEST_CONFIG,
)


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)
    initialize_db()


def initialize_db():
    db_dispenser = CoreDbDispenser()
    # Use the admin URL since we don't have a database or a user yet
    admin_db_config = CoreDbConfig(
        engine_config=DbEngineConfig(
            url=ADMIN_URL,
            engine_params=frozendict({"connect_args": frozendict({"autocommit": True})}),
        ),
        conn_type=CONNECTION_TYPE_MSSQL,
    )
    admin_db = db_dispenser.get_database(db_config=admin_db_config)
    admin_db.execute("sp_configure 'CONTAINED DATABASE AUTHENTICATION', 1")
    admin_db.execute("RECONFIGURE")
    admin_db.execute(
        "IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'test_data') "
        "BEGIN CREATE DATABASE [test_data] CONTAINMENT = PARTIAL END"
    )

    # Switch to the admin URL with the DB name
    admin_db_config = CoreDbConfig(
        engine_config=DbEngineConfig(
            url=ADMIN_W_DB_URL,
            engine_params=frozendict({"connect_args": frozendict({"autocommit": True})}),
        ),
        conn_type=CONNECTION_TYPE_MSSQL,
    )
    admin_db = db_dispenser.get_database(db_config=admin_db_config)
    admin_db.execute(
        "IF NOT EXISTS(SELECT * FROM sys.database_principals where name = 'datalens') "
        "CREATE USER datalens WITH PASSWORD = 'qweRTY123'"
    )
    admin_db.execute("GRANT CREATE TABLE, ALTER, INSERT, SELECT, UPDATE, DELETE TO datalens")


__all__ = (
    # auto-use fixtures:
    "forced_literal_use",
)
