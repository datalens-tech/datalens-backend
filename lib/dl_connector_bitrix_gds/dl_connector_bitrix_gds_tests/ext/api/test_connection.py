import json

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_bitrix_gds_tests.ext.api.base import (
    BitrixConnectionTestBase,
    BitrixDatalensConnectionTestBase,
    BitrixInvalidConnectionTestBase,
)
from dl_connector_bitrix_gds_tests.ext.config import BITRIX_PORTALS


class TestBitrixConnection(BitrixConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_portal_override(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert resp.status_code == 200, resp.json
        assert resp.json["portal"] == BITRIX_PORTALS["default"], resp.json

        new_portal = BITRIX_PORTALS["datalens"]
        resp = control_api_sync_client.put(
            url=f"/api/v1/connections/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({"portal": new_portal}),
        )
        assert resp.status_code == 200, resp.json

        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert resp.status_code == 200, resp.json
        assert resp.json["portal"] == BITRIX_PORTALS["datalens"], resp.json

        resp = control_api_sync_client.put(
            url=f"/api/v1/connections/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({"portal": BITRIX_PORTALS["default"]}),
        )
        assert resp.status_code == 200, resp.json


class TestBitrixDatalensConnection(BitrixDatalensConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestBitrixInvalidConnection(BitrixInvalidConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({}),
        )
        assert resp.status_code == 400, resp.json
