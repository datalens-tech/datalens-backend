import json

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_solomon_tests.ext.api.base import SolomonConnectionTestBase


class TestSolomonConnection(SolomonConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({}),
            headers=auth_headers,
        )
        assert resp.status_code == 200, resp.json
