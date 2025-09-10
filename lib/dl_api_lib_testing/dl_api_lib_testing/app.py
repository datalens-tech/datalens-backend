from typing import Optional

import attr
import flask

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.base_models import RequestContextInfo
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
from dl_cache_engine.primitives import CacheTTLConfig
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RequiredService
from dl_constants.enums import (
    ConnectionType,
    RLSSubjectType,
    USAuthMode,
)
from dl_core.aio.middlewares.services_registry import services_registry_middleware
from dl_core.aio.middlewares.us_manager import service_us_manager_middleware
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
import dl_retrier
from dl_rls.models import (
    RLS_FAILED_USER_NAME_PREFIX,
    RLSSubject,
)
from dl_rls.subject_resolver import BaseSubjectResolver
from dl_testing.constants import (
    TEST_USER_ID,
    TEST_USER_NAME,
)
from dl_testing.utils import get_root_certificates


@attr.s
class TestingSubjectResolver(BaseSubjectResolver):
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        """
        Mock resolver. Considers a user real if the name starts with a 'user' or
        if it equals to TEST_USER_NAME
        """
        subjects = []

        for name in names:
            subject: RLSSubject
            if name == TEST_USER_NAME:
                subject = RLSSubject(
                    subject_id=TEST_USER_ID,
                    subject_type=RLSSubjectType.user,
                    subject_name=name,
                )
            elif name.startswith("user"):
                subject = RLSSubject(subject_id="", subject_type=RLSSubjectType.user, subject_name=name)
            else:
                subject = RLSSubject(
                    subject_id="",
                    subject_type=RLSSubjectType.notfound,
                    subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
                )
            subjects.append(subject)

        return subjects

    async def get_groups_by_subject(self, rci: RequestContextInfo) -> list[str]:
        return ["_the_tests_asyncapp_group_"] if rci.user_id == TEST_USER_ID else []


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
            fake_user_id=TEST_USER_ID,
            fake_user_name=TEST_USER_NAME,
            fake_tenant=None if testing_app_settings is None else testing_app_settings.fake_tenant,
            fake_auth_data=None if testing_app_settings is None else testing_app_settings.fake_auth_data,
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
                fake_user_id=TEST_USER_ID,
                fake_user_name=TEST_USER_NAME,
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
            retry_policy_factory=dl_retrier.RetryPolicyFactory(self._settings.US_CLIENT.RETRY_POLICY),
        )

        usm_middleware_list = [
            service_us_manager_middleware(
                us_master_token=self._settings.US_MASTER_TOKEN,
                **common_us_kw,
            ),
            service_us_manager_middleware(
                us_master_token=self._settings.US_MASTER_TOKEN,
                as_user_usm=True,
                **common_us_kw,
            ),
        ]

        result = DataApiEnvSetupResult(
            auth_mw_list=auth_mw_list,
            sr_middleware_list=sr_middleware_list,
            usm_middleware_list=usm_middleware_list,
        )

        return result
