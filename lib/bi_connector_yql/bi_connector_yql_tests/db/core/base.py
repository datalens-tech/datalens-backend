import pytest

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.testcases.connection import BaseConnectionTestClass

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ
from bi_connector_yql.core.yq.us_connection import YQConnection
from bi_connector_yql.core.yq.testing.connection import make_saved_yq_connection

import bi_connector_yql_tests.db.config as test_config


class BaseYQTestClass(BaseConnectionTestClass[YQConnection]):
    conn_type = CONNECTION_TYPE_YQ
    core_test_config = test_config.CORE_TEST_CONFIG
    connection_settings = ConnectorsSettingsByType(YQ=test_config.SR_CONNECTION_SETTINGS)

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope='function')
    def connection_creation_params(self) -> dict:
        return dict(
            service_account_id=test_config.CoreConnectionSettings.SERVICE_ACCOUNT_ID,
            folder_id=test_config.CoreConnectionSettings.FOLDER_ID,
            password=test_config.CoreConnectionSettings.PASSWORD,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict,
    ) -> YQConnection:
        conn = make_saved_yq_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
        return conn
