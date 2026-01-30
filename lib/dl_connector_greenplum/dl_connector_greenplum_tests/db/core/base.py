import asyncio
from typing import Generator

import pytest
import shortuuid

from dl_core_testing.database import Db
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM
from dl_connector_greenplum.core.us_connection import GreenplumConnection
import dl_connector_greenplum_tests.db.config as test_config


class BaseGreenplumTestClass(BaseConnectionTestClass[GreenplumConnection]):
    conn_type = CONNECTION_TYPE_GREENPLUM
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        raise NotImplementedError("Must be implemented in subclass")

    @pytest.fixture(scope="class")
    def gp_partitioned_table_name(self, db: Db) -> str:
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


class GP6TestClass(BaseGreenplumTestClass):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_URLS[test_config.GreenplumVersion.GP6]

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.DB_NAME,
            host=test_config.CONNECTION_PARAMS_BY_VERSION[test_config.GreenplumVersion.GP6]["host"],
            port=test_config.CONNECTION_PARAMS_BY_VERSION[test_config.GreenplumVersion.GP6]["port"],
            username=test_config.GP_USER,
            password=test_config.GP_PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class GP7TestClass(BaseGreenplumTestClass):
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_URLS[test_config.GreenplumVersion.GP7]

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.DB_NAME,
            host=test_config.CONNECTION_PARAMS_BY_VERSION[test_config.GreenplumVersion.GP7]["host"],
            port=test_config.CONNECTION_PARAMS_BY_VERSION[test_config.GreenplumVersion.GP7]["port"],
            username=test_config.GP_USER,
            password=test_config.GP_PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )
