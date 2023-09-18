import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_monitoring_tests.db.api.base import MonitoringConnectionTestBase


class TestMonitoringConnection(MonitoringConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        pytest.skip('won\'t work with fake connection parameters')
