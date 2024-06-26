import json
from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_bitrix_gds_tests.ext.api.base import (
    BitrixConnectionTestBase,
    BitrixInvalidConnectionTestBase,
)
from dl_connector_bitrix_gds_tests.ext.config import BITRIX_PORTALS


class TestBitrixConnection(BitrixConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestBitrixDatalensConnection(BitrixConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class TestBitrixInvalidConnection(BitrixInvalidConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({}),
            headers=bi_headers,
        )
        assert resp.status_code == 400, resp.json
