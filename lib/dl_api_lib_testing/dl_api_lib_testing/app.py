import contextlib
from typing import (
    Generator,
    Optional,
)

import attr
import flask

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.flask.middlewares.trust_auth import TrustAuthService
from dl_api_lib.app.control_api.app import ControlApiAppFactory
from dl_api_lib.app.control_api.app import EnvSetupResult as ControlApiEnvSetupResult
from dl_api_lib.app.data_api.app import DataApiAppFactory
from dl_api_lib.app.data_api.app import EnvSetupResult as DataApiEnvSetupResult
from dl_api_lib.app_common import SRFactoryBuilder
from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import (
    AppSettings,
    ControlApiAppSettings,
    ControlApiAppTestingsSettings,
    DataApiAppSettings,
)
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_cache_engine.primitives import CacheTTLConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RequiredService
from dl_configs.rqe import (
    RQEBaseURL,
    RQEConfig,
)
from dl_constants.enums import (
    ConnectionType,
    RLSSubjectType,
    USAuthMode,
)
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import service_us_manager_middleware
from dl_core.rls import (
    RLS_FAILED_USER_NAME_PREFIX,
    BaseSubjectResolver,
    RLSSubject,
)
from dl_core.services_registry import ServicesRegistry
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory
from dl_core.services_registry.inst_specific_sr import (
    InstallationSpecificServiceRegistry,
    InstallationSpecificServiceRegistryFactory,
)
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.utils import FutureRef
from dl_core_testing.app_test_workarounds import TestEnvManagerFactory
from dl_core_testing.fixture_server_runner import WSGIRunner
from dl_testing.utils import get_root_certificates


@attr.s
class RQEConfigurationMaker:
    bi_test_config: ApiTestEnvironmentConfiguration = attr.ib(kw_only=True)

    @contextlib.contextmanager
    def sync_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        whitelist = self.bi_test_config.core_test_config.get_core_library_config().core_connector_ep_names
        env = dict(
            EXT_QUERY_EXECUTER_SECRET_KEY=self.bi_test_config.ext_query_executer_secret_key,
            DEV_LOGGING="1",
        )
        if whitelist:
            env["CORE_CONNECTOR_WHITELIST"] = ",".join(whitelist)

        with WSGIRunner(
            module="dl_core.bin.query_executor_sync",
            callable="app",
            ping_path="/ping",
            env=env,
        ) as runner:
            yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                host=runner.bind_addr,
                port=runner.bind_port,
            )

    @contextlib.contextmanager
    def async_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            host="127.0.0.1",
            port=65500,
        )

    @contextlib.contextmanager
    def rqe_config_subprocess_cm(self) -> Generator[RQEConfig, None, None]:
        with (
            self.sync_rqe_netloc_subprocess_cm() as sync_rqe_netloc,
            self.async_rqe_netloc_subprocess_cm() as async_rqe_netloc,
        ):
            yield RQEConfig(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                hmac_key=self.bi_test_config.ext_query_executer_secret_key.encode(),
                ext_sync_rqe=sync_rqe_netloc,
                ext_async_rqe=async_rqe_netloc,
                int_sync_rqe=sync_rqe_netloc,
                int_async_rqe=async_rqe_netloc,
            )


@attr.s
class TestingSubjectResolver(BaseSubjectResolver):
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        """
        Mock resolver. Considers a user real if the name starts with a 'user' or
        if it's equals to '_the_tests_asyncapp_user_name_'
        """
        subjects = []
        for name in names:
            if name == "_the_tests_asyncapp_user_name_":
                subjects.append(
                    RLSSubject(
                        subject_id="_the_tests_asyncapp_user_id_",
                        subject_type=RLSSubjectType.user,
                        subject_name=name,
                    )
                )
            else:
                subjects.append(
                    RLSSubject(
                        subject_id="",
                        subject_type=RLSSubjectType.user if name.startswith("user") else RLSSubjectType.notfound,
                        subject_name=name if name.startswith("user") else RLS_FAILED_USER_NAME_PREFIX + name,
                    )
                )
        return subjects


@attr.s
class TestingServiceRegistry(InstallationSpecificServiceRegistry):
    async def get_subject_resolver(self) -> BaseSubjectResolver:
        return TestingSubjectResolver()


@attr.s
class TestingServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> TestingServiceRegistry:
        return TestingServiceRegistry(service_registry_ref=sr_ref)


class TestingSRFactoryBuilder(SRFactoryBuilder[AppSettings]):
    def _get_required_services(self, settings: AppSettings) -> set[RequiredService]:
        return {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC}

    def _get_env_manager_factory(self, settings: AppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

    def _get_inst_specific_sr_factory(
        self,
        settings: AppSettings,
        ca_data: bytes,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return TestingServiceRegistryFactory()

    def _get_entity_usage_checker(self, settings: AppSettings) -> Optional[EntityUsageChecker]:
        return None

    def _get_bleeding_edge_users(self, settings: AppSettings) -> tuple[str, ...]:
        return ()

    def _get_rqe_caches_settings(self, settings: AppSettings) -> Optional[RQECachesSetting]:
        return None

    def _get_default_cache_ttl_settings(self, settings: AppSettings) -> Optional[CacheTTLConfig]:
        return None

    def _get_connector_availability(self, settings: AppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return settings.CONNECTOR_AVAILABILITY if isinstance(settings, ControlApiAppSettings) else None


class TestingControlApiAppFactory(ControlApiAppFactory[ControlApiAppSettings], TestingSRFactoryBuilder):
    """Management API app factory for tests"""

    def set_up_environment(
        self,
        app: flask.Flask,
        testing_app_settings: Optional[ControlApiAppTestingsSettings] = None,
    ) -> ControlApiEnvSetupResult:
        us_auth_mode: USAuthMode
        TrustAuthService(
            fake_user_id="_the_tests_syncapp_user_id_",
            fake_user_name="_the_tests_syncapp_user_name_",
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant,
        ).set_up(app)

        us_auth_mode_override = None if testing_app_settings is None else testing_app_settings.us_auth_mode_override

        us_auth_mode = USAuthMode.master if us_auth_mode_override is None else us_auth_mode_override

        result = ControlApiEnvSetupResult(us_auth_mode=us_auth_mode)
        return result


class TestingDataApiAppFactory(DataApiAppFactory[DataApiAppSettings], TestingSRFactoryBuilder):
    """Data API app factory for tests"""

    @property
    def _is_public(self) -> bool:
        return False

    def get_app_version(self) -> str:
        return "tests"

    def set_up_environment(
        self,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> DataApiEnvSetupResult:
        conn_opts_factory = ConnOptionsMutatorsFactory()
        ca_data = get_root_certificates()

        sr_factory = self.get_sr_factory(
            settings=self._settings,
            conn_opts_factory=conn_opts_factory,
            connectors_settings=connectors_settings,
            ca_data=ca_data,
        )

        auth_mw_list = [
            auth_trust_middleware(
                fake_user_id="_the_tests_asyncapp_user_id_",
                fake_user_name="_the_tests_asyncapp_user_name_",
            )
        ]

        sr_middleware_list = [
            services_registry_middleware(
                services_registry_factory=sr_factory,
                use_query_cache=self._settings.CACHES_ON,
                use_mutation_cache=self._settings.MUTATIONS_CACHES_ON,
                mutation_cache_default_ttl=self._settings.MUTATIONS_CACHES_DEFAULT_TTL,
            ),
        ]

        common_us_kw = dict(
            us_base_url=self._settings.US_BASE_URL,
            crypto_keys_config=self._settings.CRYPTO_KEYS_CONFIG,
            ca_data=ca_data,
        )
        usm_middleware_list = [
            service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, **common_us_kw),  # type: ignore[arg-type]
            service_us_manager_middleware(us_master_token=self._settings.US_MASTER_TOKEN, as_user_usm=True, **common_us_kw),  # type: ignore[arg-type]
        ]

        result = DataApiEnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
