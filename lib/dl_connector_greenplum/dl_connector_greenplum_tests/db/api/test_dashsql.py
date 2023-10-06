from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from dl_connector_greenplum_tests.db.api.base import GreenplumDashSQLConnectionTest
from dl_connector_greenplum_tests.db.config import DASHSQL_QUERY


class TestGreenplumDashSQL(GreenplumDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY,
        )
