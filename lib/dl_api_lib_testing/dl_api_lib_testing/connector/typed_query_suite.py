import abc
from typing import Optional

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.typed_query_base import (
    DashSQLTypedQueryTestBase,
    TypedQueryInfo,
)
from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_testing.regulated_test import RegulatedTestCase


class DefaultDashSQLTypedQueryTestSuite(DashSQLTypedQueryTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="class")
    def typed_query_info(self) -> TypedQueryInfo:
        return TypedQueryInfo(
            query_type=DashSQLQueryType.generic_query,
            query_content={"query": "select 1 as q, 2 as w, 'zxc' as e"},
            params=[],
        )

    def check_result(self, result_data: dict) -> None:
        assert result_data["data"]["rows"][0] == [1, 2, "zxc"]
        headers = result_data["data"]["headers"]
        assert len(headers) == 3
        assert [header["name"] for header in headers] == ["q", "w", "e"]
        assert [header["data_type"] for header in headers] == [
            UserDataType.integer.name,
            UserDataType.integer.name,
            UserDataType.string.name,
        ]

    @pytest.mark.asyncio
    async def test_basic_query(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        typed_query_info: TypedQueryInfo,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = await self.get_typed_query_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            typed_query_info=typed_query_info,
            headers=bi_headers,
        )
        result_data = await resp.json()
        self.check_result(result_data)
