from aiohttp.test_utils import TestClient
import attr
import pytest

from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware
from dl_api_lib.app.data_api.app import DataApiAppFactory
from dl_api_lib.app.data_api.app import EnvSetupResult as DataApiEnvSetupResult
from dl_api_lib.app_settings import DataApiAppSettings
from dl_api_lib_testing.app import TestingDataApiAppFactory
from dl_api_lib_testing.dashsql_base import DashSQLTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import ConnectionType

from bi_connector_solomon_tests.ext.api.base import SolomonConnectionTestBase


class TestSolomonDashSQL(SolomonConnectionTestBase, DashSQLTestBase):
    @pytest.fixture(scope="function")
    def data_api_app_factory(self, data_api_app_settings: DataApiAppSettings, tvm_info: str) -> DataApiAppFactory:
        class TestingDataApiAppFactoryWithBB(TestingDataApiAppFactory):
            def set_up_environment(
                self,
                connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
            ) -> DataApiEnvSetupResult:
                base_env_setup_result = super().set_up_environment(connectors_settings)
                return attr.evolve(
                    base_env_setup_result,
                    auth_mw_list=[
                        blackbox_auth_middleware(tvm_info=tvm_info),
                    ],
                )

        return TestingDataApiAppFactoryWithBB(settings=data_api_app_settings)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("dashsql_params_type", ["string", "datetime"])
    async def test_result(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        auth_headers: dict[str, str],
        dashsql_params_type: str,
    ) -> None:
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='{project="datalens", cluster="preprod_alerts", service="alerts", host="alerts"}',
            params={
                "project_id": {"type_name": "string", "value": "datalens"},
                "from": {"type_name": dashsql_params_type, "value": "2021-11-15T00:00:00+00:00"},
                "to": {"type_name": dashsql_params_type, "value": "2021-11-16T00:00:00+00:00"},
            },
            headers=auth_headers,
        )

    @pytest.mark.asyncio
    async def test_result_with_connector_specific_param(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='{project="datalens", cluster="preprod_alerts", service="alerts", host="alerts"}',
            connector_specific_params={
                "project_id": "datalens",
                "from": "2021-11-15T00:00:00+00:00",
                "to": "2021-11-16T00:00:00+00:00",
            },
            headers=auth_headers,
        )

    @pytest.mark.asyncio
    async def test_result_with_alias(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='alias(constant_line(100), "100%")',
            connector_specific_params={
                "project_id": "datalens",
                "from": "2021-11-15T00:00:00+00:00",
                "to": "2021-11-16T00:00:00+00:00",
            },
            headers=auth_headers,
        )

        resp_data = await resp.json()
        metadata = resp_data[0]["data"]
        assert "_alias" in metadata["names"]

    @pytest.mark.asyncio
    async def test_result_with_different_schemas(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=(
                'alias(series_sum("code", {project="monitoring", cluster="production", service="ui", host="cluster", '
                'code!="2*", endpoint="*", sensor="http.server.requests.status", method="*"}), "{{code}}")'
            ),
            connector_specific_params={
                "project_id": "monitoring",
                "from": "2022-02-03T00:00:00+00:00",
                "to": "2022-02-04T00:00:00+00:00",
            },
            headers=auth_headers,
        )
