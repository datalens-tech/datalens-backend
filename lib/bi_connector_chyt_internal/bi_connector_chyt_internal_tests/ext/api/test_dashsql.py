from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from bi_connector_chyt_internal_tests.ext.api.base import (
    CHYTDashSQLConnectionTest,
    CHYTUserAuthDashSQLConnectionTest,
)


class TestCHYTDashSQL(CHYTDashSQLConnectionTest, DefaultDashSQLTestSuite):
    pass


class TestCHYTUserAuthDashSQL(CHYTUserAuthDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_basic_select(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select 1, 2, 3",
            headers=auth_headers,
        )
        data = await resp.json()
        assert data[1]["data"] == [1, 2, 3]
