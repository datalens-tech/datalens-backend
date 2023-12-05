import asyncio
from typing import Generator

import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import (
    C,
    Db,
    DbTable,
    make_table,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from dl_connector_bigquery.core.testing.connection import make_bigquery_saved_connection
from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_connector_bigquery.db_testing.engine_wrapper import BigQueryDbEngineConfig
import dl_connector_bigquery_tests.ext.config as test_config


class BaseBigQueryTestClass(BaseConnectionTestClass[ConnectionSQLBigQuery]):
    conn_type = CONNECTION_TYPE_BIGQUERY
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope="function", autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self, bq_secrets) -> str:
        return f"bigquery://{bq_secrets.get_project_id()}"

    @pytest.fixture(scope="class")
    def engine_params(self, bq_secrets) -> dict:
        return dict(
            credentials_base64=bq_secrets.get_creds(),
        )

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, bq_secrets) -> BigQueryDbEngineConfig:
        return BigQueryDbEngineConfig(
            url=db_url,
            engine_params=engine_params,
            default_dataset_name=bq_secrets.get_dataset_name(),
        )

    @pytest.fixture(scope="function")
    def connection_creation_params(self, bq_secrets) -> dict:
        return dict(
            project_id=bq_secrets.get_project_id(),
            credentials=bq_secrets.get_creds(),
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> ConnectionSQLBigQuery:
        conn = make_bigquery_saved_connection(sync_usm=sync_us_manager, **connection_creation_params)
        return conn

    @pytest.fixture(scope="class")
    def sample_table(self, db: Db, bq_secrets) -> DbTable:
        # FIXME: Do this via a "smart" dispenser
        return make_table(
            db=db,
            name=bq_secrets.get_table_name(),
            schema=bq_secrets.get_dataset_name(),
            columns=[
                C(name=name, user_type=user_type) for name, user_type in TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema
            ],
            create_in_db=False,
        )
