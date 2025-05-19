import abc
from asyncio import AbstractEventLoop
from typing import (
    Any,
    ClassVar,
    Generator,
    Optional,
)

from flask.app import Flask
from flask.testing import FlaskClient
import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
    TenantDef,
)
from dl_api_lib.app.control_api.app import ControlApiAppFactory
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import (
    ControlApiAppSettings,
    ControlApiAppTestingsSettings,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.loader import preload_api_lib
from dl_api_lib_testing.app import TestingControlApiAppFactory
from dl_api_lib_testing.client import FlaskSyncApiClient
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.rqe import RQEConfig
from dl_constants.enums import (
    ConnectionType,
    QueryProcessingMode,
)
from dl_core.components.ids import FieldIdGeneratorType
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.flask_utils import (
    FlaskTestClient,
    FlaskTestResponse,
)
from dl_core_testing.rqe import RQEConfigurationMaker
from dl_testing.utils import (
    get_root_certificates,
    get_root_certificates_path,
)


class ApiTestBase(abc.ABC):
    """
    Base class defining the basic fixtures of bi-api tests
    """

    compeng_enabled: ClassVar[bool] = True
    query_processing_mode: ClassVar[QueryProcessingMode] = QueryProcessingMode.basic

    @pytest.fixture(scope="function", autouse=True)
    def preload(self) -> None:
        preload_api_lib()

    @pytest.fixture(scope="class")
    @abc.abstractmethod
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        raise NotImplementedError

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {}

    @pytest.fixture(scope="class")
    def bi_headers(self) -> Optional[dict[str, str]]:
        """Additional headers for api requests, e.g. authentication, cookies, etc"""
        return None

    def is_connector_available(self, conn_type: ConnectionType) -> bool:
        return True

    @pytest.fixture(scope="function")
    def enable_all_connectors(self, monkeypatch: Any) -> None:
        monkeypatch.setattr(ConnectorAvailabilityConfig, "check_connector_is_available", self.is_connector_available)

    @pytest.fixture(scope="function")
    def environment_readiness(self, enable_all_connectors: Any) -> None:
        """Make sure the environment is ready for tests"""
        return

    @pytest.fixture(scope="function")
    def rqe_config_subprocess(
        self,
        bi_test_config: ApiTestEnvironmentConfiguration,
    ) -> Generator[RQEConfig, None, None]:
        whitelist = bi_test_config.core_test_config.get_core_library_config().core_connector_ep_names
        with RQEConfigurationMaker(
            ext_query_executer_secret_key=bi_test_config.ext_query_executer_secret_key,
            core_connector_whitelist=whitelist,
        ).rqe_config_subprocess_cm() as rqe_config:
            yield rqe_config

    @classmethod
    def create_control_api_settings(
        cls,
        bi_test_config: ApiTestEnvironmentConfiguration,
        rqe_config_subprocess: RQEConfig,
    ) -> ControlApiAppSettings:
        core_test_config = bi_test_config.core_test_config
        us_config = core_test_config.get_us_config()

        redis_setting_maker = core_test_config.get_redis_setting_maker()

        settings = ControlApiAppSettings(  # type: ignore  # 2024-01-29 # TODO: Unexpected keyword argument "CONNECTOR_AVAILABILITY" for "ControlApiAppSettings"  [call-arg]
            CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig.from_settings(
                bi_test_config.connector_availability_settings,
            ),
            US_BASE_URL=us_config.us_host,
            US_MASTER_TOKEN=us_config.us_master_token,
            CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),
            BI_API_CONNECTOR_WHITELIST=bi_test_config.get_api_library_config().api_connector_ep_names,
            CORE_CONNECTOR_WHITELIST=core_test_config.get_core_library_config().core_connector_ep_names,
            RQE_CONFIG=rqe_config_subprocess,
            BI_COMPENG_PG_ON=cls.compeng_enabled,
            BI_COMPENG_PG_URL=core_test_config.get_compeng_url(),
            DO_DSRC_IDX_FETCH=True,
            FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,
            REDIS_ARQ=redis_setting_maker.get_redis_settings_arq(),
            FILE_UPLOADER_BASE_URL="http://127.0.0.1:9999",  # fake url
            FILE_UPLOADER_MASTER_TOKEN="qwerty",
            QUERY_PROCESSING_MODE=cls.query_processing_mode,
            CA_FILE_PATH=get_root_certificates_path(),
        )
        return settings

    @pytest.fixture(scope="function")
    def control_api_app_settings(
        self,
        bi_test_config: ApiTestEnvironmentConfiguration,
        rqe_config_subprocess: RQEConfig,
    ) -> ControlApiAppSettings:
        return self.create_control_api_settings(
            bi_test_config=bi_test_config,
            rqe_config_subprocess=rqe_config_subprocess,
        )

    @pytest.fixture(scope="class")
    def sample_table_schema(self) -> Optional[str]:
        return None

    @pytest.fixture(scope="session")
    def ca_data(self) -> bytes:
        return get_root_certificates()

    @pytest.fixture(scope="function")
    def control_api_app_factory(self, control_api_app_settings: ControlApiAppSettings) -> ControlApiAppFactory:
        return TestingControlApiAppFactory(settings=control_api_app_settings)

    @pytest.fixture(scope="function")
    def fake_tenant(self) -> TenantDef:
        return TenantCommon()

    @pytest.fixture(scope="function")
    def control_api_app(
        self,
        environment_readiness: None,
        control_api_app_factory: ControlApiAppFactory,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        fake_tenant: TenantDef,
    ) -> Generator[Flask, None, None]:
        """Session-wide test `Flask` application."""

        app = control_api_app_factory.create_app(
            connectors_settings=connectors_settings,
            testing_app_settings=ControlApiAppTestingsSettings(fake_tenant=fake_tenant),
            close_loop_after_request=False,
        )

        app.config["WE_ARE_IN_TESTS"] = True

        # Establish an application context before running the tests.
        with app.app_context() as ctx:
            assert ctx
            yield app

    @pytest.fixture(scope="function")
    def client(
        self,
        control_api_app: Flask,
        loop: AbstractEventLoop,
    ) -> FlaskClient:  # FIXME: Rename to control_app_client
        class TestClient(FlaskTestClient):
            def get_default_headers(self) -> dict[str, Optional[str]]:
                return {}

        control_api_app.test_client_class = TestClient
        # for the `json` property
        control_api_app.response_class = FlaskTestResponse  # type: ignore  # 2024-01-29 # TODO: Incompatible types in assignment (expression has type "type[FlaskTestResponse]", variable has type "type[Response]")  [assignment]

        return control_api_app.test_client()

    @pytest.fixture(scope="function")
    def control_api_sync_client(self, client: FlaskClient) -> SyncHttpClientBase:
        return FlaskSyncApiClient(int_wclient=client)

    @pytest.fixture(scope="function")
    def control_api(
        self,
        control_api_sync_client: SyncHttpClientBase,
        bi_headers: Optional[dict[str, str]],
    ) -> SyncHttpDatasetApiV1:
        return SyncHttpDatasetApiV1(client=control_api_sync_client, headers=bi_headers or {})

    @pytest.fixture(scope="function")
    def sync_us_manager(
        self,
        bi_test_config: ApiTestEnvironmentConfiguration,
        control_api_app_factory: ControlApiAppFactory,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
        control_api_app_settings: ControlApiAppSettings,
        ca_data: bytes,
    ) -> SyncUSManager:
        core_test_config = bi_test_config.core_test_config
        bi_context = RequestContextInfo.create_empty()
        us_config = core_test_config.get_us_config()

        us_manager = SyncUSManager(
            bi_context=bi_context,
            services_registry=control_api_app_factory.get_sr_factory(
                conn_opts_factory=ConnOptionsMutatorsFactory(),
                connectors_settings=connectors_settings,
                settings=control_api_app_settings,
                ca_data=ca_data,
            ).make_service_registry(request_context_info=bi_context),
            us_base_url=us_config.us_host,
            us_auth_context=USAuthContextMaster(us_config.us_master_token),
            crypto_keys_config=core_test_config.get_crypto_keys_config(),
        )
        return us_manager
