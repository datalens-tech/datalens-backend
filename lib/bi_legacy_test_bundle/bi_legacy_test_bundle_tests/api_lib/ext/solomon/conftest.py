import attr
import pytest

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import ConnectionType

from dl_api_lib.app.data_api.app import EnvSetupResult as DataApiEnvSetupResult
from bi_api_lib_testing_ya.app import TestingDataApiAppFactoryPrivate
from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware

from bi_legacy_test_bundle_tests.api_lib.app_async import create_app as create_app_async


@pytest.fixture(scope='function')
def async_api_local_env_low_level_client_with_bb(
        loop, aiohttp_client, async_app_settings_local_env, connectors_settings, tvm_info,
):
    class TestingDataApiAppFactoryWithBB(TestingDataApiAppFactoryPrivate):
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

    app = create_app_async(async_app_settings_local_env, connectors_settings, TestingDataApiAppFactoryWithBB)
    return loop.run_until_complete(aiohttp_client(app))
