import contextlib
from typing import Generator, Optional

import attr
import flask

from bi_constants.enums import ConnectionType, USAuthMode

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.enums import RedisMode, RequiredService
from bi_configs.rqe import RQEBaseURL, RQEConfig
from bi_configs.settings_submodels import RedisSettings

from bi_core.aio.middlewares.us_manager import service_us_manager_middleware
from bi_core.aio.middlewares.services_registry import services_registry_middleware
from bi_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.flask_utils.trust_auth import TrustAuthService
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting

from bi_api_lib.app_common import SRFactoryBuilder, StandaloneServiceRegistryFactory
from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import (
    AppSettings,
    ControlApiAppSettings,
    DataApiAppSettings,
    ControlApiAppTestingsSettings,
)
from bi_api_lib.app.control_api.app import EnvSetupResult as ControlApiEnvSetupResult, ControlApiAppFactory
from bi_api_lib.app.data_api.app import DataApiAppFactory, EnvSetupResult as DataApiEnvSetupResult
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_core_testing.app_test_workarounds import TestEnvManagerFactory
from bi_core_testing.fixture_server_runner import WSGIRunner
from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration


@attr.s
class RQEConfigurationMaker:
    bi_test_config: BiApiTestEnvironmentConfiguration = attr.ib(kw_only=True)

    @contextlib.contextmanager
    def sync_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        with WSGIRunner(
                module='bi_core.bin.query_executor_sync',
                callable='app',
                ping_path='/ping',
                env=dict(
                    EXT_QUERY_EXECUTER_SECRET_KEY=self.bi_test_config.ext_query_executer_secret_key,
                    DEV_LOGGING='1',
                ),
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
                self.async_rqe_netloc_subprocess_cm() as async_rqe_netloc
        ):
            yield RQEConfig(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                hmac_key=self.bi_test_config.ext_query_executer_secret_key.encode(),
                ext_sync_rqe=sync_rqe_netloc,
                ext_async_rqe=async_rqe_netloc,
                int_sync_rqe=sync_rqe_netloc,
                int_async_rqe=async_rqe_netloc,
            )


@attr.s
class RedisSettingMaker:
    bi_test_config: BiApiTestEnvironmentConfiguration = attr.ib(kw_only=True)

    def get_redis_settings(self, db: int) -> RedisSettings:
        return RedisSettings(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            MODE=RedisMode.single_host,
            CLUSTER_NAME='',
            HOSTS=(self.bi_test_config.redis_host,),
            PORT=self.bi_test_config.redis_port,
            DB=db,
            PASSWORD=self.bi_test_config.redis_password,
        )

    def get_redis_settings_default(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_default)

    def get_redis_settings_cache(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_cache)

    def get_redis_settings_mutation(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_mutation)

    def get_redis_settings_arq(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_arq)


class TestingSRFactoryBuilder(SRFactoryBuilder[AppSettings]):
    def _get_required_services(self, settings: AppSettings) -> set[RequiredService]:
        return {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC}

    def _get_env_manager_factory(self, settings: AppSettings) -> EnvManagerFactory:
        return TestEnvManagerFactory()

    def _get_inst_specific_sr_factory(
            self, settings: AppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        return StandaloneServiceRegistryFactory()

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
            fake_user_id='_the_tests_syncapp_user_id_',
            fake_user_name='_the_tests_syncapp_user_name_',
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant
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
        return 'tests'

    def set_up_environment(
            self,
            connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> DataApiEnvSetupResult:

        conn_opts_factory = ConnOptionsMutatorsFactory()
        sr_factory = self.get_sr_factory(
            settings=self._settings, conn_opts_factory=conn_opts_factory, connectors_settings=connectors_settings
        )

        auth_mw_list = [
            auth_trust_middleware(
                fake_user_id='_the_tests_asyncapp_user_id_',
                fake_user_name='_the_tests_asyncapp_user_name_',
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
