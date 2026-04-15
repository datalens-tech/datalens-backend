import asyncio
from typing import Generator

import pytest

from dl_core_testing.database import (
    C,
    Db,
    DbTable,
    make_table,
)
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from dl_connector_bigquery.db_testing.engine_wrapper import BigQueryDbEngineConfig
import dl_connector_bigquery_tests.ext.config as test_config
from dl_connector_bigquery_tests.ext.settings import Settings


class BaseBigQueryTestClass(BaseConnectionTestClass[ConnectionSQLBigQuery]):
    conn_type = CONNECTION_TYPE_BIGQUERY
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
    def db_url(self, settings: Settings) -> str:
        return f"bigquery://{settings.BIGQUERY_CONFIG['project_id']}"

    @pytest.fixture(scope="class")
    def engine_params(self, settings: Settings) -> dict:
        return dict(
            credentials_base64=settings.BIGQUERY_CREDS,
        )

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, settings: Settings) -> BigQueryDbEngineConfig:
        return BigQueryDbEngineConfig(
            url=db_url,
            engine_params=engine_params,
            default_dataset_name=settings.BIGQUERY_CONFIG["dataset_name"],
        )

    @pytest.fixture(scope="function")
    def connection_creation_params(self, settings: Settings) -> dict:
        return dict(
            project_id=settings.BIGQUERY_CONFIG["project_id"],
            credentials=settings.BIGQUERY_CREDS,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope="class")
    def sample_table(self, db: Db, settings: Settings) -> DbTable:
        # FIXME: Do this via a "smart" dispenser
        return make_table(
            db=db,
            name=settings.BIGQUERY_CONFIG["table_name"],
            schema=settings.BIGQUERY_CONFIG["dataset_name"],
            columns=[
                C(name=name, user_type=user_type) for name, user_type in TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema
            ],
            create_in_db=False,
        )
