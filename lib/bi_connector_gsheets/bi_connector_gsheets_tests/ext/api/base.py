from contextlib import contextmanager
import os
from typing import Generator

import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_core_testing.database import (
    CoreDbConfig,
    Db,
)
from dl_core_testing.engine_wrapper import TestingEngineWrapper

from bi_connector_gsheets.core.constants import (
    CONNECTION_TYPE_GSHEETS,
    SOURCE_TYPE_GSHEETS,
)
import bi_connector_gsheets.core.gozora
from bi_connector_gsheets.core.us_connection import (
    GSheetsConnection,
    GSheetsConnectOptions,
)
from bi_connector_gsheets_tests.ext.config import (
    API_TEST_CONFIG,
    GSHEETS_EXAMPLE_URL,
)


class GsheetsConnectionTestBase(ConnectionTestBase):
    conn_type = CONNECTION_TYPE_GSHEETS

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return ""

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Db:
        engine_wrapper = TestingEngineWrapper(config=db_config.engine_config)
        return Db(config=db_config, engine_wrapper=engine_wrapper)

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(url=GSHEETS_EXAMPLE_URL)


class GsheetsGozoraConnectionTestBase(GsheetsConnectionTestBase):
    @contextmanager
    def gozora_enabled(self) -> Generator[None, None, None]:
        old_get_conn_options = GSheetsConnection.get_conn_options
        old_tvm_info_and_secret = bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret

        def new_get_conn_options(self) -> GSheetsConnectOptions:
            conn_options = old_get_conn_options(self)
            return conn_options

        try:
            GSheetsConnection.get_conn_options = new_get_conn_options
            bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret = os.environ["TVM_INFO"].split()
            yield
        finally:
            GSheetsConnection.get_conn_options = old_get_conn_options
            bi_connector_gsheets.core.gozora.TvmCliSingletonGoZora.tvm_info_and_secret = old_tvm_info_and_secret

    @contextmanager
    def create_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
    ) -> Generator[str, None, None]:
        with self.gozora_enabled():
            with super().create_connection(control_api_sync_client, connection_params) as conn_id:
                yield conn_id


class GsheetsInvalidConnectionTestBase(GsheetsConnectionTestBase):
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(url="lol" + GSHEETS_EXAMPLE_URL)


class GsheetsDatasetTestBase(GsheetsConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(parameters=dict(), source_type=SOURCE_TYPE_GSHEETS.name, title="current sheet")


class GsheetsDataApiTestBase(GsheetsDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_on = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("eol", "release"),
            summable_field="A",
            range_field="A",
            distinct_field="codename",
            date_field="created",
        )
