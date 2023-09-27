import abc

import pytest

from bi_api_lib_testing_ya.app import TestingDataApiAppFactoryPrivate
from bi_api_lib_testing_ya.base import ApiTestPrivateBase
from bi_api_lib_testing_ya.configuration import ApiTestEnvironmentConfigurationPrivate
from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    YCAuthSettings,
)
from bi_cloud_integration.iam_mock import IAMServicesMockFacade
from bi_defaults.yenv_type import AppType
from dl_api_lib.app.data_api.app import DataApiAppFactory
from dl_api_lib.app_settings import DataApiAppSettings
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_configs.rqe import RQEConfig
from dl_core.utils import attrs_evolve_to_subclass


class DataApiTestPrivateBase(DataApiTestBase, ApiTestPrivateBase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="function")
    def data_api_app_factory(self, data_api_app_settings: DataApiAppSettings) -> DataApiAppFactory:
        return TestingDataApiAppFactoryPrivate(settings=data_api_app_settings)

    @pytest.fixture(scope="function")
    def data_api_app_settings(  # type: ignore[override]
        self,
        bi_test_config: ApiTestEnvironmentConfigurationPrivate,
        rqe_config_subprocess: RQEConfig,
        iam_services_mock: IAMServicesMockFacade,
    ) -> AsyncAppSettings:
        base_settings = self.create_data_api_settings(
            bi_test_config=bi_test_config,
            rqe_config_subprocess=rqe_config_subprocess,
        )

        return attrs_evolve_to_subclass(
            AsyncAppSettings,
            base_settings,
            APP_TYPE=AppType.TESTS,
            PUBLIC_API_KEY=None,
            YC_AUTH_SETTINGS=YCAuthSettings(
                YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
                YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
                YC_AUTHORIZE_PERMISSION=None,
            ),  # type: ignore
            YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,
        )
