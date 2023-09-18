import asyncio
from typing import Generator

import pytest
import shortuuid

from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import Db
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql.testing.connection import make_postgresql_saved_connection
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
import dl_connector_postgresql_tests.db.config as test_config


class BasePostgreSQLTestClass(BaseConnectionTestClass[ConnectionPostgreSQL]):
    conn_type = CONNECTION_TYPE_POSTGRES
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
            username=test_config.CoreConnectionSettings.USERNAME,
            password=test_config.CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> ConnectionPostgreSQL:
        conn = make_postgresql_saved_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params,
        )
        return conn

    @pytest.fixture(scope="class")
    def pg_partitioned_table_name(self, db: Db) -> str:
        name = f"test_partitioned_table_{shortuuid.uuid().lower()}"
        queries = [
            f"""
                create table {name}
                (ts timestamptz not null, value text)
                partition by range(ts)
            """,
            f"""
                create table {name}_01
                partition of {name}
                for values from ('2020-01-01 00:00:00') to ('2021-01-01 00:00:00')
            """,
            f"""
                insert into {name} (ts, value)
                values ('2020-01-01 01:02:03', 'a')
            """,
        ]
        for query in queries:
            db.execute(query)
        yield name
        db.execute(f"drop table if exists {name}")


class BaseSslPostgreSQLTestClass(BasePostgreSQLTestClass):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_SSL_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreSslConnectionSettings.DB_NAME,
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            password=test_config.CoreSslConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
            ssl_enable=True,
            ssl_ca=test_config.get_postgres_ssl_ca(),
        )
