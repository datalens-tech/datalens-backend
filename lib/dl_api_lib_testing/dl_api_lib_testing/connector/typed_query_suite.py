import abc
from typing import Optional

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.typed_query_base import (
    DashSQLTypedQueryTestBase,
    TypedQueryInfo,
)
from dl_constants.enums import DashSQLQueryType
from dl_testing.regulated_test import RegulatedTestCase


class DefaultDashSQLTypedQueryTestSuite(DashSQLTypedQueryTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="class")
    def typed_query_headers(self) -> Optional[dict[str, str]]:
        return None

    @pytest.fixture(scope="class")
    def typed_query_info(self) -> TypedQueryInfo:
        return TypedQueryInfo(
            query_type=DashSQLQueryType.classic_query,
            query_content={"query": "select 1, 2, 3"},
            params=[],
        )

    def check_result(self, result_data: dict) -> None:
        assert result_data["data"][0] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_basic_query(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        typed_query_info: TypedQueryInfo,
        typed_query_headers: Optional[dict[str, str]],
    ) -> None:
        resp = await self.get_typed_query_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            typed_query_info=typed_query_info,
            headers=typed_query_headers,
        )
        result_data = await resp.json()
        self.check_result(result_data)
