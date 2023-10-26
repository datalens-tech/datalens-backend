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


class SnowFlakeConnectionTestBase(BaseSnowFlakeTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_SNOWFLAKE

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params_native(self, sf_secrets) -> dict:
        return dict(
            account_name=sf_secrets.get_account_name(),
            user_name=sf_secrets.get_user_name(),
            user_role=sf_secrets.get_user_role(),
            client_id=sf_secrets.get_client_id(),
            client_secret=sf_secrets.get_client_secret(),
            db_name=sf_secrets.get_database(),
            schema=sf_secrets.get_schema(),
            warehouse=sf_secrets.get_warehouse(),
            refresh_token=sf_secrets.get_refresh_token_x(),
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
    def dataset_params(self, sf_secrets) -> dict:
        return dict(
            source_type=SOURCE_TYPE_SNOWFLAKE_TABLE.name,
            parameters=dict(
                table_name=sf_secrets.get_table_name(),
            ),
        )


class SnowFlakeDataApiTestBase(SnowFlakeDatasetTestBase, StandardizedDataApiTestBase):
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("Category", "City"),
            summable_field="Sales",
            range_field="Sales",
            distinct_field="City",
            date_field="Order Date",
        )
