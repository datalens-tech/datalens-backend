import abc
from typing import Optional

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.dashsql_base import DashSQLTestBase
from dl_testing.regulated_test import RegulatedTestCase


class DefaultDashSQLTestSuite(DashSQLTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="class")
    def dashsql_headers(self) -> Optional[dict[str, str]]:
        return None

    @pytest.fixture(scope="class")
    def dashsql_basic_query(self) -> str:
        return "select 1, 2, 3"

    @pytest.mark.asyncio
    async def test_basic_select(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_basic_query: str,
        dashsql_headers: Optional[dict[str, str]],
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=dashsql_basic_query,
            headers=dashsql_headers,
        )
        data = await resp.json()
        assert data[1]["data"] == [1, 2, 3]
