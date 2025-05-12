from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_testing.test_data.sql_queries import DASHSQL_EXAMPLE_PARAMS

from dl_connector_trino_tests.db.api.base import TrinoDashSQLConnectionTest
from dl_connector_trino_tests.db.config import (
    QUERY_WITH_PARAMS,
    SUBSELECT_QUERY,
)


class TestTrinoDashSQL(TrinoDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=SUBSELECT_QUERY,
        )

        resp_data = await resp.json()

        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == [
            "number",
            "num_bool",
            "num_tinyint",
            "num_smallint",
            "num_integer",
            "num_bigint",
            "num_real",
            "num_double",
            "num_decimal_9_3",
            "char_3",
            "varchar_3",
            "json",
            "date",
            "timestamp",
            "timestamp_with_timezone",
        ]
        assert resp_data[0]["data"]["driver_types"] == [
            "integer",
            "boolean",
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "decimal(9, 3)",
            "char(3)",
            "varchar(3)",
            "json",
            "date",
            "timestamp(3)",
            "timestamp(3) with time zone",
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "integer",
            "boolean",
            "smallint",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "decimal",
            "char",
            "varchar",
            "type_decorator",
            "date",
            "timestamp",
            "timestamp",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
            "boolean",
            "integer",
            "integer",
            "integer",
            "integer",
            "float",
            "float",
            "float",
            "string",
            "string",
            "string",
            "date",
            "genericdatetime",
            "genericdatetime",
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
            "boolean",
            "boolean",
            "date",
            "genericdatetime",
            "string",
            "string",
            "boolean",
            "boolean",
            "boolean",
            "boolean",
            "integer",
        ], resp_data[0]["data"]
        assert resp_data[1]["event"] == "row"
        print(resp_data[1]["data"])
        assert resp_data[1]["data"] == pytest.approx(
            [
                "normal ':string'",
                "extended:string\nwith\nnewlines",
                "some\\:string\nwith\\stuff",
                562949953421312,
                73786976294838206464.5,
                True,
                False,
                "2021-07-19",
                "2021-07-19 19:35:43.000000",
                "11",
                "22",
                True,
                False,
                True,
                False,
                1,
            ]
        ), resp_data[1]["data"]
        assert resp_data[2]["event"] == "footer"

    @pytest.mark.asyncio
    async def test_invalid_alias(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        """
        Trino doesn't support unicode aliases
        """

        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="SELECT 1 AS русский_текст",
            fail_ok=True,
        )

        assert resp.status == 400
        resp_data = await resp.json()
        print(resp_data)
        assert resp_data["code"] == "ERR.DS_API.DB.INVALID_QUERY"
        assert resp_data["message"] == "Invalid SQL query to the database."
        assert "Expecting: <identifier>" in resp_data["details"]["db_message"]
