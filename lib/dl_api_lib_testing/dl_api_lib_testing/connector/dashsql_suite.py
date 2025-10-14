import abc
from typing import Optional

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib.common_models.data_export import DataExportForbiddenReason
from dl_api_lib_testing.dashsql_base import DashSQLTestBase
from dl_testing.regulated_test import RegulatedTestCase


class DefaultDashSQLTestSuite(DashSQLTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="class")
    def dashsql_basic_query(self) -> str:
        return "select 1, 2, 3"

    @pytest.mark.asyncio
    async def test_basic_select(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_basic_query: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=dashsql_basic_query,
            headers=bi_headers,
        )
        data = await resp.json()
        assert data[1]["data"] == [1, 2, 3]

        metadata = data[0]["data"]
        assert not metadata["data_export"]["background"]["allowed"]
        assert DataExportForbiddenReason.prohibited_in_dashsql.value in metadata["data_export"]["background"]["reason"]

    @pytest.mark.asyncio
    async def test_basic_select_with_new_resp_schema(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_basic_query: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=dashsql_basic_query,
            headers=bi_headers,
            with_export_info=True,
        )
        data = await resp.json()
        assert data["events"][1]["data"] == [1, 2, 3]
        assert not data["data_export"]["background"]["allowed"]
        assert DataExportForbiddenReason.prohibited_in_dashsql.value in data["data_export"]["background"]["reason"]
