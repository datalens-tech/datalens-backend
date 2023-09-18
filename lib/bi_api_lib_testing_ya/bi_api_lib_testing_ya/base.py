import abc
from typing import Any, ClassVar, Generator, Type

import pytest

from dl_configs.enums import AppType, EnvType
from dl_configs.rqe import RQEConfig

from dl_core.utils import attrs_evolve_to_subclass
from bi_api_lib_ya.app_settings import ControlPlaneAppSettings, YCAuthSettings
from dl_api_lib.app.control_api.app import ControlApiAppFactory

from bi_testing_ya.iam_mock import apply_iam_services_mock
from bi_cloud_integration.iam_mock import IAMServicesMockFacade

from dl_api_lib_testing.base import BiApiTestBase
from bi_api_lib_testing_ya.app import TestingControlApiAppFactoryPrivate
from bi_api_lib_testing_ya.configuration import BiApiTestEnvironmentConfigurationPrivate


class BiApiTestPrivateBase(BiApiTestBase, abc.ABC):
    control_api_app_factory_cls: ClassVar[Type[ControlApiAppFactory]] = TestingControlApiAppFactoryPrivate

    @pytest.fixture(scope='function')
    def iam_services_mock(self, monkeypatch: Any) -> Generator[IAMServicesMockFacade, None, None]:
        yield from apply_iam_services_mock(monkeypatch)

    @pytest.fixture(scope='function')
    def control_api_app_settings(  # type: ignore[override]
            self,
            bi_test_config: BiApiTestEnvironmentConfigurationPrivate,
            rqe_config_subprocess: RQEConfig,
            iam_services_mock: IAMServicesMockFacade,
    ) -> ControlPlaneAppSettings:
        base_settings = self.create_control_api_settings(
            bi_test_config=bi_test_config,
            rqe_config_subprocess=rqe_config_subprocess,
        )

        return attrs_evolve_to_subclass(
            ControlPlaneAppSettings,
            base_settings,
            APP_TYPE=AppType.TESTS,
            ENV_TYPE=EnvType.development,
            DLS_HOST=bi_test_config.dls_host,
            DLS_API_KEY=bi_test_config.dls_key,
            YC_AUTH_SETTINGS=YCAuthSettings(
                YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
                YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
                YC_AUTHORIZE_PERMISSION=None,
            ),  # type: ignore
            YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,
        )
