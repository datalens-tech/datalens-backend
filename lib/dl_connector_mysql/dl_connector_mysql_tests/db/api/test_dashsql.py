from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_testing.test_data.sql_queries import DASHSQL_EXAMPLE_PARAMS

from dl_connector_mysql_tests.db.api.base import MySQLDashSQLConnectionTest
from dl_connector_mysql_tests.db.config import (
    QUERY_WITH_PARAMS,
    SUBSELECT_QUERY_FULL,
)


class TestMySQLDashSQL(MySQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
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
            "num",
            "num_unsigned",
            "num_signed",
            "num_decimal",
            "num_decimal_12_2",
            "num_text",
            "num_binary",
            "num_char",
            "num_date",
            "num_datetime",
            "num_nchar",
            "num_time",
            "int_30bit",
            "some_double",
        ]
        assert resp_data[0]["data"]["driver_types"] == [
            8,
            8,
            8,
            246,
            246,
            253,
            253,
            253,
            10,
            12,
            253,
            11,
            8,
            5,
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "bigint",
            "bigint",
            "bigint",
            "decimal",
            "decimal",
            "text",
            "text",
            "text",
            "date",
            "datetime",
            "text",
            "time",
            "bigint",
            "double",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
            "integer",
            "integer",
            "float",
            "float",
            "string",
            "string",
            "string",
            "date",
            "genericdatetime",
            "string",
            "unsupported",
            "integer",
            "float",
        ]

    @pytest.mark.asyncio
    async def test_result_with_params(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=QUERY_WITH_PARAMS,
            params=DASHSQL_EXAMPLE_PARAMS,
        )

        resp_data = await resp.json()
        assert len(resp_data) == 3, resp_data
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["bi_types"] == [
            "string",
            "string",
            "string",
            "integer",
            "float",
            "integer",
            "integer",
            "date",
            "genericdatetime",
            "string",
            "string",
            "integer",
            "integer",
            "integer",
            "integer",
            "integer",
        ], resp_data[0]["data"]
        assert resp_data[1]["event"] == "row"
        assert resp_data[1]["data"] == pytest.approx(
            [
                "normal ':string'",
                "extended:string\nwith\nnewlines",
                "some\\:string\nwith\\stuff",
                562949953421312,
                73786976294838206464.5,
                1,
                0,
                "2021-07-19",
                "2021-07-19T19:35:43",
                "11",
                "22",
                1,
                0,
                1,
                0,
                1,
            ]
        ), resp_data[1]["data"]
        assert resp_data[2]["event"] == "footer"

    @pytest.mark.asyncio
    async def test_percent_char(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select concat(0, '%') as value",
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == [
            "value",
        ]
        assert resp_data[1]["event"] == "row"
        assert resp_data[1]["data"] == [
            "0%",
        ]
