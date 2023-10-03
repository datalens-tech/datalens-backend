import json

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_gsheets_tests.ext.api.base import (
    GsheetsConnectionTestBase,
    GsheetsGozoraConnectionTestBase,
    GsheetsInvalidConnectionTestBase,
)


class TestGsheetsConnection(GsheetsConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestGsheetsGozoraConnection(GsheetsGozoraConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestGsheetsInvalidConnection(GsheetsInvalidConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({}),
        )
        assert resp.status_code == 400, resp.json
        assert resp.json["code"] == "ERR.DS_API.CONNECTION_CONFIG", resp.json
        assert resp.json["message"].startswith("Invalid URL in the connection"), resp.json
