from aiohttp.test_utils import TestClient
import pytest

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from dl_connector_promql_tests.db.api.base import PromQLConnectionTestBase


class TestPromQLDashSQL(PromQLConnectionTestBase, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_basic_select(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query="select 1, 2, 3",
            fail_ok=True,
        )

        assert resp.status == 400
        resp_data = await resp.json()
        assert resp_data["code"] == "ERR.DS_API.DB"
        assert resp_data["details"]["db_message"] == "'step', 'from', 'to' must be in parameters"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "dashsql_params_type",
        [
            "string",
            "datetime",
        ],
    )
    async def test_result(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        dashsql_params_type: str,
    ) -> None:
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='{job="nodeexporter", type={{data_type}}}',
            params={
                "from": {"type_name": dashsql_params_type, "value": "2021-09-28T17:00:00Z"},
                "to": {"type_name": dashsql_params_type, "value": "2021-09-28T18:00:00Z"},
                "step": {"type_name": "string", "value": "5m"},
                "data_type": {"type_name": "string", "value": "test_data"},
            },
        )

    @pytest.mark.asyncio
    async def test_result_with_connector_specific_params(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
    ) -> None:
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='{job="nodeexporter", type={{data_type}}}',
            params={"data_type": {"type_name": "string", "value": "test_data"}},
            connector_specific_params={
                "step": "5m",
                "from": "2021-09-28T17:00:00Z",
                "to": "2021-09-28T18:00:00Z",
            },
        )
