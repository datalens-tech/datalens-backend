from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_core_testing.database import DbTable

from dl_connector_ydb_tests.db.api.base import YDBDashSQLConnectionTest
from dl_connector_ydb_tests.db.config import DASHSQL_QUERY


class TestYDBDashSQL(YDBDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY.format(table_name=sample_table.name),
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"][:18] == [
            "id",
            "some_str",
            "some_utf8",
            "some_int",
            "some_uint8",
            "some_int64",
            "some_uint64",
            "some_float",
            "some_double",
            "some_bool",
            "some_date",
            "some_datetime",
            "some_datetime64",
            "some_timestamp",
            "some_timestamp64",
            "some_interval",
            "some_interval64",
            "some_uuid",
        ]
        assert resp_data[0]["data"]["driver_types"][:18] == [
            "int32?",
            "string",
            "utf8?",
            "int32",
            "uint8?",
            "int64",
            "uint64",
            "float",
            "double",
            "bool",
            "date",
            "datetime",
            "datetime64",
            "timestamp",
            "timestamp64",
            "interval",
            "interval64",
            "uuid?",
        ]
        assert resp_data[0]["data"]["db_types"][:18] == [
            "int32",
            "string",
            "utf8",
            "int32",
            "uint8",
            "int64",
            "uint64",
            "float",
            "double",
            "boolean",
            "date",
            "timestamp",
            "datetime64",
            "timestamp",
            "timestamp64",
            "interval",
            "interval64",
            "uuid",
        ]
        assert resp_data[0]["data"]["bi_types"][:18] == [
            "integer",
            "string",
            "string",
            "integer",
            "integer",
            "integer",
            "integer",
            "float",
            "float",
            "boolean",
            "date",
            "genericdatetime",
            "genericdatetime",
            "genericdatetime",
            "genericdatetime",
            "integer",
            "integer",
            "uuid",
        ]

        assert resp_data[-1]["event"] == "footer", resp_data[-1]

    @pytest.mark.asyncio
    async def test_interval(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='SELECT Interval("P0DT1.0S") as interval_value',
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"] == [
            "interval_value",
        ]
        assert resp_data[0]["data"]["driver_types"] == [
            "interval",
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "interval",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
        ]

        assert resp_data[1]["event"] == "row", resp_data
        assert resp_data[1]["data"] == [1_000_000]

    @pytest.mark.asyncio
    async def test_result_with_error(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select 1/",
            fail_ok=True,
        )

        resp_data = await resp.json()
        assert resp.status == 400, resp_data
        assert resp_data["code"] == "ERR.DS_API.DB", resp_data
        assert resp_data.get("details", {}).get("db_message"), resp_data
