import abc
from typing import (
    ClassVar,
    Type,
)

import pytest

from bi_api_lib_testing_ya.app import TestingDataApiAppFactoryPrivate
from bi_api_lib_testing_ya.base import BiApiTestPrivateBase
from bi_api_lib_testing_ya.configuration import BiApiTestEnvironmentConfigurationPrivate
from bi_api_lib_ya.app_settings import (
    AsyncAppSettings,
    YCAuthSettings,
)
from bi_cloud_integration.iam_mock import IAMServicesMockFacade
from dl_api_lib.app.data_api.app import DataApiAppFactory
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_configs.enums import AppType
from dl_configs.rqe import RQEConfig
from dl_core.utils import attrs_evolve_to_subclass


class DataApiTestPrivateBase(DataApiTestBase, BiApiTestPrivateBase, metaclass=abc.ABCMeta):
    data_api_app_factory_cls: ClassVar[Type[DataApiAppFactory]] = TestingDataApiAppFactoryPrivate

    @pytest.fixture(scope="function")
    def data_api_app_settings(  # type: ignore[override]
        self,
        bi_test_config: BiApiTestEnvironmentConfigurationPrivate,
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
