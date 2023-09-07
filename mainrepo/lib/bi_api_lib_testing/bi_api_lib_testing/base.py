import abc
from asyncio import AbstractEventLoop
from typing import Any, ClassVar, Generator, Optional, Type

from flask.app import Flask
from flask.testing import FlaskClient
import pytest

from bi_constants.enums import ConnectionType

from bi_configs.rqe import RQEConfig
from bi_configs.connectors_settings import ConnectorSettingsBase

from bi_api_commons.base_models import TenantCommon

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase

from bi_core.components.ids import FieldIdGeneratorType
from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase
from bi_core_testing.flask_utils import FlaskTestResponse, FlaskTestClient

from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.app.control_api.app import ControlApiAppFactoryBase
from bi_api_lib.app_settings import MDBSettings, ControlPlaneAppTestingsSettings, ControlApiAppSettings
from bi_api_lib.loader import ApiLibraryConfig, preload_bi_api_lib, load_bi_api_lib

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.app import RQEConfigurationMaker, RedisSettingMaker, TestingControlApiAppFactory
from bi_api_lib_testing.client import FlaskSyncApiClient


class BiApiTestBase(abc.ABC):
    """
    Base class defining the basic fixtures of bi-api tests
    """

    control_api_app_factory_cls: ClassVar[Type[ControlApiAppFactoryBase]] = TestingControlApiAppFactory
    bi_compeng_pg_on: ClassVar[bool] = True

    @pytest.fixture(scope='function', autouse=True)
    def preload(self):
        preload_bi_api_lib()

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
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {}

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

    @classmethod
    def create_control_api_settings(
            cls,
            bi_test_config: BiApiTestEnvironmentConfiguration,
            rqe_config_subprocess: RQEConfig,
    ) -> ControlApiAppSettings:
        core_test_config = bi_test_config.core_test_config
        us_config = core_test_config.get_us_config()

        redis_setting_maker = RedisSettingMaker(bi_test_config=bi_test_config)

        settings = ControlApiAppSettings(
            CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig.from_settings(
                bi_test_config.connector_availability_settings,
            ),

            US_BASE_URL=us_config.us_host,
            US_MASTER_TOKEN=us_config.us_master_token,
            CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),

            CONNECTOR_WHITELIST=tuple(bi_test_config.connector_whitelist.split(',')),  # FIXME: make a separate classvar

            RQE_CONFIG=rqe_config_subprocess,
            BI_COMPENG_PG_ON=cls.bi_compeng_pg_on,
            BI_COMPENG_PG_URL=bi_test_config.bi_compeng_pg_url,

            DO_DSRC_IDX_FETCH=True,

            FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,

            REDIS_ARQ=redis_setting_maker.get_redis_settings_arq(),

            FILE_UPLOADER_BASE_URL='http://127.0.0.1:9999',  # fake url
            FILE_UPLOADER_MASTER_TOKEN='qwerty',
            MDB=MDBSettings(),
        )
        return settings

    @pytest.fixture(scope='function')
    def control_api_app_settings(
            self,
            bi_test_config: BiApiTestEnvironmentConfiguration,
            rqe_config_subprocess: RQEConfig,
    ) -> ControlApiAppSettings:
        return self.create_control_api_settings(
            bi_test_config=bi_test_config,
            rqe_config_subprocess=rqe_config_subprocess,
        )

    @pytest.fixture(scope='function')
    def control_api_app(
            self,
            environment_readiness: None,
            control_api_app_settings: ControlApiAppSettings,
            connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> Generator[Flask, None, None]:
        """Session-wide test `Flask` application."""

        control_app_factory = self.control_api_app_factory_cls(settings=control_api_app_settings)
        load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=control_api_app_settings.CONNECTOR_WHITELIST))
        app = control_app_factory.create_app(
            connectors_settings=connectors_settings,
            testing_app_settings=ControlPlaneAppTestingsSettings(
                fake_tenant=TenantCommon()
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
