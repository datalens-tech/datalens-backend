from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_testing.test_data.sql_queries import DASHSQL_EXAMPLE_PARAMS

from dl_connector_oracle_tests.db.api.base import OracleDashSQLConnectionTest
from dl_connector_oracle_tests.db.config import (
    QUERY_WITH_PARAMS,
    SUBSELECT_QUERY_FULL,
)


class TestOracleDashSQL(OracleDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.fixture(scope="class")
    def dashsql_basic_query(self) -> str:
        return "select 1, 2, 3 from dual"

    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=SUBSELECT_QUERY_FULL,
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == [
            "NUM",
            "NUM_STR",
            "NUM_INTEGER",
            "NUM_NUMBER",
            "NUM_BINARY_FLOAT",
            "NUM_BINARY_DOUBLE",
            "NUM_CHAR",
            "NUM_VARCHAR",
            "NUM_VARCHAR2",
            "NUM_NCHAR",
            "NUM_NVARCHAR2",
            "NUM_DATE",
            "NUM_TIMESTAMP",
            "NUM_TIMESTAMP_TZ",
        ]
        assert resp_data[0]["data"]["driver_types"] == [
            "db_type_number",
            "db_type_varchar",
            "db_type_number",
            "db_type_number",
            "db_type_binary_float",
            "db_type_binary_double",
            "db_type_char",
            "db_type_varchar",
            "db_type_varchar",
            "db_type_nchar",
            "db_type_nvarchar",
            "db_type_date",
            "db_type_timestamp",
            "db_type_timestamp_tz",
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "integer",
            "varchar",
            "integer",
            "integer",
            "binary_float",
            "binary_double",
            "char",
            "varchar",
            "varchar",
            "nchar",
            "nvarchar",
            "date",
            "timestamp",
            "timestamp",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
            "string",
            "integer",
            "integer",
            "float",
            "float",
            "string",
            "string",
            "string",
            "string",
            "string",
            "genericdatetime",
            "genericdatetime",
            "genericdatetime",
        ]

    @pytest.mark.asyncio
    async def test_result_with_params(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=QUERY_WITH_PARAMS,
            params=DASHSQL_EXAMPLE_PARAMS,
        )
