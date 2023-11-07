from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from dl_connector_clickhouse_tests.db.api.base import ClickHouseDashSQLConnectionTest
from dl_connector_clickhouse_tests.db.config import (
    DASHSQL_QUERY,
    DASHSQL_QUERY_FULL,
)


class TestClickHouseDashSQL(ClickHouseDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY,
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == ["a", "b", "ts"]
        assert resp_data[0]["data"]["driver_types"] == ["Nullable(UInt8)", "Array(UInt8)", "Nullable(DateTime('UTC'))"]
        assert resp_data[0]["data"]["db_types"] == ["uint8", "array", "datetime"]
        assert resp_data[0]["data"]["bi_types"] == ["integer", "unsupported", "genericdatetime"]

        assert resp_data[0]["data"]["clickhouse_headers"]["X-ClickHouse-Timezone"] == "UTC", resp_data
        assert resp_data[1] == {"event": "row", "data": [11, [33, 44], "2020-01-02 03:04:16"]}, resp_data
        assert resp_data[2] == {"event": "row", "data": [22, [33, 44], "2020-01-02 03:04:27"]}, resp_data
        assert resp_data[-1]["event"] == "footer", resp_data
        assert isinstance(resp_data[-1]["data"], dict)

    @pytest.mark.asyncio
    async def test_result_extended(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY_FULL,
        )

    @pytest.mark.asyncio
    async def test_invalid_alias(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="SELECT 1 AS русский текст",
            fail_ok=True,
        )

        assert resp.status == 400
        resp_data = await resp.json()
        assert resp_data["code"] == "ERR.DS_API.DB.INVALID_QUERY"
        assert resp_data["message"] == "Invalid SQL query to the database."
        assert "Unrecognized token" in resp_data["details"]["db_message"]
        assert "русский текст" in resp_data["details"]["db_message"]
