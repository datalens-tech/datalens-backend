import re

from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import RawSQLLevel


class TestDashSQL(DefaultApiTestBase, DefaultDashSQLTestSuite):
    raw_sql_level = RawSQLLevel.dashsql

    ISO_DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")

    @pytest.fixture(scope="class")
    def dashsql_datetime_query(self) -> str:
        return """select 
            toDateTime('2023-06-15 14:30:45'), 
            toDateTime64('2023-06-15 14:30:45.123', 3),
            toDateTime('2023-06-15 14:30:45', 'Europe/Moscow'),
            toDateTime64('2023-06-15 14:30:45.123', 3, 'UTC'),
            toDateTime(1686838245),
            cast('2023-06-15 14:30:45' AS DateTime),
            now()
            """

    @pytest.fixture(scope="class")
    def dashsql_array_query(self) -> str:
        return "select [1, 2, 3]"

    @pytest.mark.asyncio
    async def test_postprocess_datetime(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_datetime_query: str,
        bi_headers: dict[str, str] | None,
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=dashsql_datetime_query,
            headers=bi_headers,
        )
        data = await resp.json()
        row_data = data[1]["data"]
        for datetime_value in row_data:
            assert self.ISO_DATETIME_PATTERN.match(
                datetime_value
            ), f"Value '{datetime_value}' doesn't match ISO 8601 format"

    @pytest.mark.asyncio
    async def test_postprocess_array(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_array_query: str,
        bi_headers: dict[str, str] | None,
    ) -> None:
        """Test that array values are JSON-dumped to strings."""
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=dashsql_array_query,
            headers=bi_headers,
        )
        data = await resp.json()

        row_data = data[1]["data"]
        assert row_data[0] == [1, 2, 3], "Array should be returned as list"
