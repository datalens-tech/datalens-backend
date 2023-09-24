import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_connector_chyt.core.settings import CHYTConnectorSettings
from dl_constants.enums import (
    ConnectionType,
    RawSQLLevel,
)

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)
from bi_connector_chyt_internal_tests.ext.config import BI_TEST_CONFIG
from bi_connector_chyt_internal_tests.ext.core.base import (
    BaseCHYTTestClass,
    BaseCHYTUserAuthTestClass,
)


class CHYTConnectionTestBase(BaseCHYTTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_CH_OVER_YT
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, yt_token: str) -> dict:
        return dict(
            alias="*chyt_datalens_back",
            cluster="hahn",
            raw_sql_level=RawSQLLevel.dashsql.name,
            token=yt_token,
        )

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {
            CONNECTION_TYPE_CH_OVER_YT: CHYTConnectorSettings(),
        }


class CHYTUserAuthConnectionTestBase(BaseCHYTUserAuthTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def auth_headers(self, yt_token: str) -> dict[str, str]:
        return {
            "Authorization": f"OAuth {yt_token}",
        }

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return BI_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            alias="*chyt_datalens_back",
            cluster="hahn",
            raw_sql_level=RawSQLLevel.dashsql.name,
        )

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {
            CONNECTION_TYPE_CH_OVER_YT_USER_AUTH: CHYTConnectorSettings(),
        }


class CHYTDashSQLConnectionTest(CHYTConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql


class CHYTUserAuthDashSQLConnectionTest(CHYTUserAuthConnectionTestBase):
    raw_sql_level = RawSQLLevel.dashsql
