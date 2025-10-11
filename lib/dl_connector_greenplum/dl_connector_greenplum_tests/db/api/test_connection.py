from typing import Optional
import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.api.base import GreenplumConnectionTestBase


class TestGreenplumSQLConnection(GreenplumConnectionTestBase, DefaultConnectorConnectionTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorConnectionTestSuite.test_connection_sources: (
                "Fails because we use PG instead of GP: "
                'psycopg2.errors.UndefinedTable: relation "pg_partitions" does not exist'
            ),
        },
    )

    def test_create_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        assert saved_connection_id
        response = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
            headers=bi_headers,
        )
        assert response.status_code == 200
        assert response.json["db_type"] == "greenplum"
        assert response.json["cache_ttl_sec"] is None

    @pytest.mark.parametrize(
        "args,expected_status_code",
        [
            ("", 200),
            ("?search_text=ampl&db_name=test_memory_catalog", 400),
            ("?limit=1&db_name=test_memory_catalog", 400),
            ("?offset=1&db_name=test_memory_catalog", 400),
            ("?search_text=ampl&limit=1&offset=0&db_name=test_memory_catalog", 400),
            ("?search_text=ampl", 200),
            ("?limit=1", 200),
            ("?offset=1", 200),
            ("?offset=0", 200),
            ("?search_text=ampl&limit=1&offset=1", 200),
        ],
    )
    def test_connection_sources_paginated_with_params(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: dict[str, str] | None,
        args: str,
        expected_status_code: int,
    ) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}/info/sources{args}",
            headers=bi_headers,
        )
        assert resp.status_code == expected_status_code, resp.json
        resp_data = resp.json
        if expected_status_code == 200:
            assert "sources" in resp_data, resp_data
            assert isinstance(resp_data["sources"], list), resp_data
            assert resp_data["sources"], resp_data
        else:
            assert resp_data.get("code") == "ERR.DS_API.INVALID_REQUEST"
            assert resp_data.get("message")
