import abc

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.dashsql_base import DashSQLTestBase
from dl_testing.regulated_test import RegulatedTestCase


class DefaultDashSQLTestSuite(DashSQLTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    @pytest.mark.asyncio
    async def test_basic_select(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select 1, 2, 3",
        )
        data = await resp.json()
        assert data[1]["data"] == [1, 2, 3]
