from __future__ import annotations

import pytest

from bi_constants.enums import ConnectionType

from bi_configs.connectors_settings import ConnectorsSettingsByType

from bi_api_commons.base_models import RequestContextInfo

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.testcases.connection import BaseConnectionTestClass

from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection
from bi_connector_usage_tracking_ya_team.core.testing.connection import make_saved_usage_tracking_ya_team_connection

import bi_connector_usage_tracking_ya_team_tests.db.config as common_test_config
import bi_connector_usage_tracking_ya_team_tests.db.core.config as test_config


class BaseUsageTrackingYaTeamTestClass(BaseConnectionTestClass[UsageTrackingYaTeamConnection]):
    conn_type = ConnectionType.usage_tracking_ya_team
    core_test_config = common_test_config.CORE_TEST_CONFIG
    connection_settings = ConnectorsSettingsByType(USAGE_TRACKING_YA_TEAM=test_config.SR_CONNECTION_SETTINGS)

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return common_test_config.DB_CORE_URL

    @pytest.fixture(scope='session')
    def conn_bi_context(self) -> RequestContextInfo:
        return RequestContextInfo.create(
            request_id=None,
            tenant=None,
            user_id='datalens_user_id',
            user_name='datalens',
            x_dl_debug_mode=False,
            endpoint_code=None,
            x_dl_context=None,
            plain_headers={},
            secret_headers={},
            auth_data=None,
        )

    @pytest.fixture(scope='function')
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=common_test_config.CoreConnectionSettings.DB_NAME,
            endpoint=common_test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=common_test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=common_test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict,
    ) -> UsageTrackingYaTeamConnection:
        return make_saved_usage_tracking_ya_team_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
