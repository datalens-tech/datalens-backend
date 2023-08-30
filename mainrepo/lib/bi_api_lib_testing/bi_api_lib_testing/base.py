import abc
from asyncio import AbstractEventLoop
from typing import Any, ClassVar, Generator, Optional

from flask.app import Flask
from flask.testing import FlaskClient
import pytest

from bi_constants.enums import ConnectionType

from bi_configs.enums import AppType, EnvType
from bi_configs.rqe import RQEConfig
from bi_configs.connectors_settings import ConnectorsSettingsByType
from bi_configs.settings_submodels import YCAuthSettings

from bi_testing_ya.iam_mock import apply_iam_services_mock
from bi_cloud_integration.iam_mock import IAMServicesMockFacade

from bi_api_commons.base_models import TenantYCFolder

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase

from bi_core.components.ids import FieldIdGeneratorType
from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase
from bi_core_testing.flask_utils import FlaskTestResponse, FlaskTestClient

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.app_settings import ControlPlaneAppSettings, MDBSettings, ControlPlaneAppTestingsSettings

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.app import RQEConfigurationMaker, RedisSettingMaker, TestingControlApiAppFactory
from bi_api_lib_testing.client import FlaskSyncApiClient


class BiApiTestBase(abc.ABC):
    """
    Base class defining the basic fixtures of bi-api tests
    """

    bi_compeng_pg_on: ClassVar[bool] = True

    @pytest.fixture(scope='class')
    @abc.abstractmethod
    def bi_test_config(self) -> BiApiTestEnvironmentConfiguration:
        raise NotImplementedError

    @pytest.fixture(scope='class')
    def core_test_config(
            self, bi_test_config: BiApiTestEnvironmentConfiguration,
    ) -> CoreTestEnvironmentConfigurationBase:
        return bi_test_config.core_test_config

    @pytest.fixture(scope='class')
    def connectors_settings(self) -> ConnectorsSettingsByType:
        return ConnectorsSettingsByType()

    def is_connector_available(self, conn_type: ConnectionType) -> bool:
        return True

    @pytest.fixture(scope='function')
    def enable_all_connectors(self, monkeypatch: Any) -> None:
        monkeypatch.setattr(ConnectorAvailabilityConfig, 'check_connector_is_available', self.is_connector_available)

    @pytest.fixture(scope='function')
    def environment_readiness(self, enable_all_connectors: Any) -> None:
        """Make sure the environment is ready for tests"""

    @pytest.fixture(scope='function')
    def rqe_config_subprocess(
            self, bi_test_config: BiApiTestEnvironmentConfiguration,
    ) -> Generator[RQEConfig, None, None]:
        with RQEConfigurationMaker(bi_test_config=bi_test_config).rqe_config_subprocess_cm() as rqe_config:
            yield rqe_config

    @pytest.fixture(scope='function')
    def iam_services_mock(self, monkeypatch: Any) -> Generator[IAMServicesMockFacade, None, None]:
        yield from apply_iam_services_mock(monkeypatch)

    @pytest.fixture(scope='function')
    def control_api_app_settings(
            self,
            bi_test_config: BiApiTestEnvironmentConfiguration,
            rqe_config_subprocess: RQEConfig,
            iam_services_mock: IAMServicesMockFacade,
            connectors_settings: ConnectorsSettingsByType,
    ) -> ControlPlaneAppSettings:

        core_test_config = bi_test_config.core_test_config
        us_config = core_test_config.get_us_config()

        redis_setting_maker = RedisSettingMaker(bi_test_config=bi_test_config)

        settings = ControlPlaneAppSettings(
            APP_TYPE=AppType.TESTS,
            ENV_TYPE=EnvType.development,

            US_BASE_URL=us_config.us_host,
            US_MASTER_TOKEN=us_config.us_master_token,
            CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),

            DLS_HOST=bi_test_config.dls_host,
            DLS_API_KEY=bi_test_config.dls_key,

            YC_AUTH_SETTINGS=YCAuthSettings(
                YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
                YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
                YC_AUTHORIZE_PERMISSION=None,
            ),  # type: ignore
            YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,
            CONNECTORS=connectors_settings,

            RQE_CONFIG=rqe_config_subprocess,
            BI_COMPENG_PG_ON=self.bi_compeng_pg_on,
            BI_COMPENG_PG_URL=bi_test_config.bi_compeng_pg_url,

            DO_DSRC_IDX_FETCH=True,

            FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,

            REDIS_ARQ=redis_setting_maker.get_redis_settings_arq(),

            FILE_UPLOADER_BASE_URL='http://127.0.0.1:9999',  # fake url
            FILE_UPLOADER_MASTER_TOKEN='qwerty',
            MDB=MDBSettings(),
        )  # type: ignore
        return settings

    @pytest.fixture(scope='function')
    def control_api_app(
            self,
            environment_readiness: None,
            control_api_app_settings: ControlPlaneAppSettings,
    ) -> Generator[Flask, None, None]:
        """Session-wide test `Flask` application."""

        control_app_factory = TestingControlApiAppFactory()
        app = control_app_factory.create_app(
            control_api_app_settings,
            testing_app_settings=ControlPlaneAppTestingsSettings(
                fake_tenant=TenantYCFolder(folder_id='folder_1')
            ),
            close_loop_after_request=False,
        )

        app.config['WE_ARE_IN_TESTS'] = True

        # Establish an application context before running the tests.
        with app.app_context() as ctx:
            assert ctx
            yield app

    @pytest.fixture(scope='function')
    def client(self, control_api_app: Flask, loop: AbstractEventLoop) -> FlaskClient:  # FIXME: Rename to control_app_client
        class TestClient(FlaskTestClient):
            def get_default_headers(self) -> dict[str, Optional[str]]:
                return {}

        control_api_app.test_client_class = TestClient
        control_api_app.response_class = FlaskTestResponse  # for the `json` property

        return control_api_app.test_client()

    @pytest.fixture(scope='function')
    def control_api_sync_client(self, client: FlaskClient) -> SyncHttpClientBase:
        return FlaskSyncApiClient(int_wclient=client)
