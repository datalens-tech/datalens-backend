import datetime

import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase

from dl_connector_snowflake.core.constants import (
    CONNECTION_TYPE_SNOWFLAKE,
    SOURCE_TYPE_SNOWFLAKE_TABLE,
)
from dl_connector_snowflake_tests.ext.config import API_TEST_CONFIG
from dl_connector_snowflake_tests.ext.core.base import BaseSnowFlakeTestClass
from dl_connector_snowflake_tests.ext.settings import Settings


class SnowFlakeConnectionTestBase(BaseSnowFlakeTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_SNOWFLAKE

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params_native(self, settings: Settings) -> dict:
        return dict(
            account_name=settings.SNOWFLAKE_CONFIG["account_name"],
            user_name=settings.SNOWFLAKE_CONFIG["user_name"],
            user_role=settings.SNOWFLAKE_CONFIG["user_role"],
            client_id=settings.SNOWFLAKE_CONFIG["client_id"],
            client_secret=settings.SNOWFLAKE_CLIENT_SECRET,
            db_name=settings.SNOWFLAKE_CONFIG["database"],
            schema=settings.SNOWFLAKE_CONFIG["schema"],
            warehouse=settings.SNOWFLAKE_CONFIG["warehouse"],
            refresh_token=settings.SNOWFLAKE_REFRESH_TOKEN_X,
            refresh_token_expire_time=datetime.datetime.now() + datetime.timedelta(days=80),
        )

    @pytest.fixture(scope="class")
    def connection_params(self, connection_params_native) -> dict:
        to_serialize = {k: v for k, v in connection_params_native.items()}
        # 2014-12-22T03:12:58
        if connection_params_native["refresh_token_expire_time"]:
            dt = connection_params_native["refresh_token_expire_time"]
            to_serialize["refresh_token_expire_time"] = dt.strftime("%Y-%m-%dT%H:%M:%S")

        return to_serialize


class SnowFlakeDatasetTestBase(SnowFlakeConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, settings: Settings) -> dict:
        return dict(
            source_type=SOURCE_TYPE_SNOWFLAKE_TABLE.name,
            parameters=dict(
                table_name=settings.SNOWFLAKE_CONFIG["table_name"],
            ),
        )


class SnowFlakeDataApiTestBase(SnowFlakeDatasetTestBase, StandardizedDataApiTestBase):
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("Category", "City"),
            summable_field="Sales",
            range_field="Sales",
            distinct_field="City",
            date_field="Order Date",
        )
