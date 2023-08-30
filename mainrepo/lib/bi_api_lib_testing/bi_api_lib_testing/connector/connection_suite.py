import json

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase

from bi_api_lib_testing.connection_base import ConnectionTestBase


class DefaultConnectorConnectionTestSuite(ConnectionTestBase):
    def test_create_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        assert saved_connection_id
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert resp.status_code == 200, resp.json

    def test_test_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type='application/json',
            data=json.dumps({}),
        )
        assert resp.status_code == 200, resp.json
