import asyncio
from typing import Generator

import pytest

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse.testing.connection import make_clickhouse_saved_connection
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import Db
from dl_core_testing.dataset_builder import (
    DatasetBuilderFactory,
    DefaultDbDatasetBuilderFactory,
)
from dl_core_testing.testcases.dataset import BaseDatasetTestClass
import dl_core_tests.db.config as test_config


class DefaultCoreTestClass(BaseDatasetTestClass[ConnectionClickhouse]):
    """Base class for generic, non-connectorized core tests"""

    conn_type = CONNECTION_TYPE_CLICKHOUSE
    source_type = SOURCE_TYPE_CH_TABLE

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

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

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
    ) -> ConnectionClickhouse:
        conn = make_clickhouse_saved_connection(sync_usm=sync_us_manager, **connection_creation_params)
        return conn

    @pytest.fixture(scope="function")
    def dataset_builder_factory(
        self,
        sync_us_manager: SyncUSManager,
        conn_default_service_registry: ServicesRegistry,
        db: Db,
        saved_connection: ConnectionBase,
    ) -> DatasetBuilderFactory:
        return DefaultDbDatasetBuilderFactory(
            service_registry=conn_default_service_registry,
            sync_us_manager=sync_us_manager,
            db=db,
            connection=saved_connection,
        )
