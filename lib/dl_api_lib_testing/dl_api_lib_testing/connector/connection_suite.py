import json
from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_testing.regulated_test import RegulatedTestCase


class DefaultConnectorConnectionTestSuite(ConnectionTestBase, RegulatedTestCase):
    def test_create_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        assert saved_connection_id
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
            headers=bi_headers,
        )
        assert resp.status_code == 200, resp.json

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
        assert resp.status_code == 200, resp.json

    def test_cache_ttl_sec_override(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert resp.status_code == 200, resp.json
        assert resp.json["cache_ttl_sec"] is None, resp.json

        cache_ttl_override = 100500
        resp = control_api_sync_client.put(
            url=f"/api/v1/connections/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({"cache_ttl_sec": cache_ttl_override}),
            headers=bi_headers,
        )
        assert resp.status_code == 200, resp.json

        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert resp.status_code == 200, resp.json
        assert resp.json["cache_ttl_sec"] == cache_ttl_override, resp.json

    def test_connection_options(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
            headers=bi_headers,
        )
        assert resp.status_code == 200, resp.json
        assert isinstance(resp.json["options"]["allow_dashsql_usage"], bool)
        assert isinstance(resp.json["options"]["allow_dataset_usage"], bool)
        assert isinstance(resp.json["options"]["allow_typed_query_usage"], bool)
        assert len(resp.json["options"]["query_types"]) > 0
        assert any([qt["query_type"] == "generic_query" for qt in resp.json["options"]["query_types"]])
