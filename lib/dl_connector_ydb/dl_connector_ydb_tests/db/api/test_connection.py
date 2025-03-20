from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib.app_settings import ControlApiAppSettings
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_constants.api_constants import DLHeadersCommon
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_ydb_tests.db.api.base import YDBConnectionTestBase


class TestYDBConnection(YDBConnectionTestBase, DefaultConnectorConnectionTestSuite):
    # a separate test since password=self.data.token
    def test_export_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
        sync_us_manager: SyncUSManager,
        control_api_app_settings: ControlApiAppSettings,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, expected_type=ConnectionBase)
        assert isinstance(conn, ConnectionBase)

        us_master_token = control_api_app_settings.US_MASTER_TOKEN
        assert us_master_token

        if bi_headers is None:
            bi_headers = dict()

        bi_headers[DLHeadersCommon.US_MASTER_TOKEN.value] = us_master_token

        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/export/{saved_connection_id}",
            headers=bi_headers,
        )

        if not conn.allow_export:
            assert resp.status_code == 400
            return

        assert resp.status_code == 200, resp.json
        if hasattr(conn.data, "token"):
            token = resp.json["connection"]["token"]
            assert token == "******"
