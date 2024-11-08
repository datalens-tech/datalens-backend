import asyncio
import os
from typing import Generator

from frozendict import frozendict
import pytest

import dl_configs.utils as bi_configs_utils
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
import dl_connector_clickhouse_tests.db.config as test_config


class BaseClickHouseTestClass(BaseConnectionTestClass[ConnectionClickhouse]):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
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


class BaseClickHouseDefaultUserTestClass(BaseClickHouseTestClass):
    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            host=test_config.CoreConnectionSettings.HOST,
            port=test_config.CoreConnectionSettings.PORT,
        )


class BaseClickHouseReadonlyUserTestClass(BaseClickHouseTestClass):
    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreReadonlyConnectionSettings.DB_NAME,
            host=test_config.CoreReadonlyConnectionSettings.HOST,
            port=test_config.CoreReadonlyConnectionSettings.PORT,
            username=test_config.CoreReadonlyConnectionSettings.USERNAME,
            password=test_config.CoreReadonlyConnectionSettings.PASSWORD,
            readonly=1,
        )


class BaseSslClickHouseTestClass(BaseClickHouseTestClass):
    @pytest.fixture(autouse=True)
    def clear_ssl_folder(self):
        folder_path = bi_configs_utils.get_temp_root_certificates_folder_path()

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            os.unlink(file_path)

    def check_ssl_directory_is_empty(self) -> None:
        assert not os.listdir(bi_configs_utils.get_temp_root_certificates_folder_path())

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_SSL_URL

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        return {
            "connect_args": frozendict(
                verify=test_config.get_clickhouse_ssl_ca_path(),
            )
        }

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreSslConnectionSettings.DB_NAME,
            host=test_config.CoreSslConnectionSettings.HOST,
            port=test_config.CoreSslConnectionSettings.PORT,
            username=test_config.CoreSslConnectionSettings.USERNAME,
            password=test_config.CoreSslConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
            secure=True,
            ssl_ca=test_config.get_clickhouse_ssl_ca(),
        )
