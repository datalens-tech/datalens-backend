from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from dl_connector_mssql_tests.db.api.base import MSSQLDashSQLConnectionTest
from dl_connector_mssql_tests.db.config import SUBSELECT_QUERY_FULL


class TestMSSQLDashSQL(MSSQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
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
            "number",
            "num_tinyint",
            "num_smallint",
            "num_integer",
            "num_bigint",
            "num_float",
            "num_real",
            "num_numeric",
            "num_decimal",
            "num_bit",
            "num_char",
            "num_varchar",
            "num_text",
            "num_nchar",
            "num_nvarchar",
            "num_ntext",
            "num_date",
            "num_datetime",
            "num_datetime2",
            "num_smalldatetime",
            "num_datetimeoffset",
            "uuid",
        ]
        assert resp_data[0]["data"]["driver_types"] == [
            "int",
            "int",
            "int",
            "int",
            "int",
            "float",
            "float",
            "decimal",
            "decimal",
            "bool",
            "str",
            "str",
            "str",
            "str",
            "str",
            "str",
            "date",
            "datetime",
            "datetime",
            "datetime",
            "str",
            "str",
        ]
        assert resp_data[0]["data"]["db_types"] == [
            "integer",
            "integer",
            "integer",
            "integer",
            "integer",
            "float",
            "float",
            "decimal",
            "decimal",
            "bit",
            "ntext",
            "ntext",
            "ntext",
            "ntext",
            "ntext",
            "ntext",
            "date",
            "datetime",
            "datetime",
            "datetime",
            "ntext",
            "ntext",
        ]
        assert resp_data[0]["data"]["bi_types"] == [
            "integer",
            "integer",
            "integer",
            "integer",
            "integer",
            "float",
            "float",
            "float",
            "float",
            "boolean",
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
            "string",
            "genericdatetime",
            "string",
            "genericdatetime",
            "string",
            "string",
        ]
