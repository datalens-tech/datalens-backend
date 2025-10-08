import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_trino_tests.db.api.base import TrinoConnectionTestBase


class TestTrinoConnection(TrinoConnectionTestBase, DefaultConnectorConnectionTestSuite):
    @pytest.mark.parametrize(
        "args,expected_status",
        [
            ("", 200),
            ("?search_text=ampl&db_name=test_memory_catalog", 200),
            ("?limit=1&db_name=test_memory_catalog", 200),
            ("?offset=1&db_name=test_memory_catalog", 200),
            ("?search_text=ampl&limit=1&offset=0&db_name=test_memory_catalog", 200),
            ("?search_text=ampl", 400),
            ("?limit=1", 200),
            ("?offset=1", 400),
            ("?offset=0", 200),
            ("?search_text=ampl&limit=1&offset=1", 400),
        ],
    )
    def test_connection_sources_paginated_with_params(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: dict[str, str] | None,
        args: str,
        expected_status: int,
    ) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}/info/sources{args}",
            headers=bi_headers,
        )
        assert resp.status_code == expected_status, resp.json
        resp_data = resp.json
        if expected_status == 200:
            assert "sources" in resp_data, resp_data
            assert isinstance(resp_data["sources"], list), resp_data
            assert resp_data["sources"], resp_data["sources"]
        else:
            assert resp_data.get("code") == "ERR.DS_API.INVALID_REQUEST"
            assert resp_data.get("message")
