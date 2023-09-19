from __future__ import annotations

import pytest

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_api_lib_tests.db.config import (
    BI_TEST_CONFIG,
    DB_CORE_URL,
    CoreConnectionSettings,
)
from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig


class DefaultApiTestBase(DataApiTestBase, ConnectionTestBase):
    """The knowledge that this is a ClickHouse connector should not go beyond this class"""

    bi_compeng_pg_on = True
    conn_type = CONNECTION_TYPE_CLICKHOUSE

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CORE_URL

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope="function")
    def environment_readiness(self, enable_all_connectors) -> None:
        pass

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
        )

    @pytest.fixture(scope="session")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name="test_data",
                table_name="sample_superstore",
            ),
        )

    @pytest.fixture(scope="function")
    def dataset_id(self, saved_dataset: Dataset) -> str:
        return saved_dataset.id
