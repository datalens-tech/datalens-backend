from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_constants.enums import UserDataType
from dl_core_testing.database import DbTable

from dl_connector_ydb_tests.db.api.base import YDBDashSQLConnectionTest


class TestYDBParametrizedQL(YDBDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_parametrized_ql_chart_select_as_is(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        parametrized_query = """
        $add_param = ($text) -> {
            RETURN $text || " " || {{text_value}};
        };
        SELECT
            {{int_value}} as id,
            $add_param("sample") as text
        ;
        """

        params = {
            "int_value": {
                "type_name": UserDataType.integer.name,
                "value": 10,
            },
            "text_value": {
                "type_name": UserDataType.string.name,
                "value": "test",
            },
        }

        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=parametrized_query,
            params=params,
            fail_ok=True,
        )

        resp_data = await resp.json()

        assert resp.status == 200, f"Unexpected status code: {resp.status}"

        assert resp_data[0]["event"] == "metadata", resp_data
        assert "id" in resp_data[0]["data"]["names"]
