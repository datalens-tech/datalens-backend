from __future__ import annotations

import pytest

from bi_cloud_integration.yc_as_client import DLASClient

from bi_api_commons.base_models import RequestContextInfo

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_ch_filtered.usage_tracking.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING,
)
from bi_connector_bundle_ch_filtered.usage_tracking.core.us_connection import UsageTrackingConnection
from bi_connector_bundle_ch_filtered.usage_tracking.core.testing.connection import make_saved_usage_tracking_connection
from bi_connector_bundle_ch_filtered.usage_tracking.core.testing.lifecycle import authorize_mock
from bi_core_testing.testcases.connection import BaseConnectionTestClass
from bi_core.exc import PlatformPermissionRequired

import bi_connector_bundle_ch_filtered_tests.db.config as common_test_config
import bi_connector_bundle_ch_filtered_tests.db.usage_tracking.config as test_config


class BaseUsageTrackingTestClass(BaseConnectionTestClass[UsageTrackingConnection]):
    conn_type = CONNECTION_TYPE_USAGE_TRACKING
    core_test_config = common_test_config.CORE_TEST_CONFIG
    connection_settings = test_config.SR_CONNECTION_SETTINGS
    inst_specific_sr_factory = common_test_config.YC_SR_FACTORY

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return common_test_config.DB_CORE_URL

    @pytest.fixture(scope='session')
    def conn_bi_context(self) -> RequestContextInfo:
        return test_config.RCI

    @pytest.fixture(scope='function')
    def connection_creation_params(self) -> dict:
        return dict(
            endpoint=common_test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=common_test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=common_test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict, monkeypatch
    ) -> UsageTrackingConnection:
        monkeypatch.setattr(DLASClient, 'authorize', authorize_mock)
        conn = make_saved_usage_tracking_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
        try:
            return sync_us_manager.get_by_id(conn.uuid)  # to invoke a lifecycle manager
        except PlatformPermissionRequired:
            return conn


class UsageTrackingTestClassWithWrongAuth(BaseUsageTrackingTestClass):
    @pytest.fixture(scope='session')
    def conn_bi_context(self) -> RequestContextInfo:
        return test_config.RCI_WITH_WRONG_AUTH
