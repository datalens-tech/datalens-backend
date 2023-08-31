from __future__ import annotations

import abc
import logging.config
from typing import TYPE_CHECKING, Optional, final

from bi_cloud_integration.sa_creds import SACredsSettings, SACredsRetrieverFactory

from bi_task_processor.arq_wrapper import create_arq_redis_settings

from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_configs.enums import AppType, RequiredService, RQE_SERVICES
from bi_constants.enums import ConnectionType

from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_i18n.localizer_base import LocalizerLoader
from bi_core.services_registry.entity_checker import EntityUsageChecker
from bi_core.services_registry.env_manager_factory import (
    CloudEnvManagerFactory, IntranetEnvManagerFactory, DataCloudEnvManagerFactory,
)
from bi_core.services_registry.file_uploader_client_factory import FileUploaderSettings
from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core_testing.app_test_workarounds import TestEnvManagerFactory
from bi_core.mdb_utils import MDBDomainManagerSettings

from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistryFactory
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistryFactory

from bi_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from bi_api_lib.app_settings import BaseAppSettings, AsyncAppSettings
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.connector_availability.main import get_connector_availability_config_for_env
from bi_api_lib.i18n.registry import LOCALIZATION_CONFIGS
from bi_api_lib.public.entity_usage_checker import PublicEnvEntityUsageChecker
from bi_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from bi_api_lib.service_registry.sr_factory import DefaultBiApiSRFactory

if TYPE_CHECKING:
    from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory


LOGGER = logging.getLogger(__name__)


class SRFactoryBuilder(abc.ABC):
    IS_ASYNC_ENV: bool

    @abc.abstractmethod
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_inst_specific_sr_factory(
            self, settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_rqe_caches_settings(self, settings: BaseAppSettings) -> Optional[RQECachesSetting]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_default_cache_ttl_settings(self, settings: BaseAppSettings) -> Optional[CacheTTLConfig]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        raise NotImplementedError

    @final
    def get_sr_factory(
        self,
        settings: BaseAppSettings,
        conn_opts_factory: ConnOptionsMutatorsFactory,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> DefaultBiApiSRFactory:
        supported_functions_manager = SupportedFunctionsManager(app_type=settings.APP_TYPE)

        file_uploader_settings = FileUploaderSettings(
            base_url=settings.FILE_UPLOADER_BASE_URL,
            master_token=settings.FILE_UPLOADER_MASTER_TOKEN,
        ) if settings.FILE_UPLOADER_BASE_URL and settings.FILE_UPLOADER_MASTER_TOKEN else None

        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )

        required_services: set[RequiredService] = set(self._get_required_services(settings))
        if settings.BI_COMPENG_PG_ON:
            required_services.add(RequiredService.POSTGRES)

        localization_loader = LocalizerLoader(configs=LOCALIZATION_CONFIGS)
        localization_factory = localization_loader.load()
        localizer_fallback = localization_factory.get_for_locale(
            locale=settings.DEFAULT_LOCALE
        ) if settings.DEFAULT_LOCALE else None

        sr_factory = DefaultBiApiSRFactory(
            async_env=self.IS_ASYNC_ENV,
            rqe_config=settings.RQE_CONFIG,
            default_cache_ttl_config=self._get_default_cache_ttl_settings(settings),
            bleeding_edge_users=self._get_bleeding_edge_users(settings),
            connect_options_factory=conn_opts_factory,
            env_manager_factory=self._get_env_manager_factory(settings),
            default_formula_parser_type=settings.FORMULA_PARSER_TYPE,
            connectors_settings=connectors_settings,
            entity_usage_checker=self._get_entity_usage_checker(settings),
            field_id_generator_type=settings.FIELD_ID_GENERATOR_TYPE,
            file_uploader_settings=file_uploader_settings,
            redis_pool_settings=create_arq_redis_settings(settings.REDIS_ARQ) if settings.REDIS_ARQ else None,
            mdb_domain_manager_settings=mdb_domain_manager_settings,
            rqe_caches_settings=self._get_rqe_caches_settings(settings),
            supported_functions_manager=supported_functions_manager,
            required_services=required_services,
            localizer_factory=localization_factory,
            localizer_fallback=localizer_fallback,
            connector_availability=self._get_connector_availability(settings),
            inst_specific_sr_factory=self._get_inst_specific_sr_factory(settings),
            force_non_rqe_mode=settings.RQE_FORCE_OFF,
        )
        return sr_factory


class LegacySRFactoryBuilder(SRFactoryBuilder):
    def _get_required_services(self, settings: BaseAppSettings) -> set[RequiredService]:
        required_services: set[RequiredService] = {  # type: ignore
            AppType.CLOUD: RQE_SERVICES,
            AppType.CLOUD_PUBLIC: RQE_SERVICES,
            AppType.CLOUD_EMBED: RQE_SERVICES,
            AppType.INTRANET: {RequiredService.RQE_INT_SYNC},
            AppType.TESTS: {RequiredService.RQE_INT_SYNC, RequiredService.RQE_EXT_SYNC},
            AppType.DATA_CLOUD: set(),
            AppType.NEBIUS: RQE_SERVICES,
        }.get(settings.APP_TYPE, set())
        return required_services

    def _get_env_manager_factory(self, settings: BaseAppSettings) -> EnvManagerFactory:
        if settings.APP_TYPE == AppType.INTRANET:
            return IntranetEnvManagerFactory()
        elif settings.APP_TYPE == AppType.TESTS:
            # TODO FIX: BI-2517 switch to normal DataCloud mode
            return TestEnvManagerFactory()
        elif settings.APP_TYPE == AppType.DATA_CLOUD:
            return DataCloudEnvManagerFactory(
                samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS),
            )
        else:
            return CloudEnvManagerFactory(
                samples_ch_hosts=list(settings.SAMPLES_CH_HOSTS),
            )

    def _get_inst_specific_sr_factory(
            self,
            settings: BaseAppSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        if settings.APP_TYPE == AppType.INTRANET:
            return YTServiceRegistryFactory(
                dls_host=settings.DLS_HOST,
                dls_api_key=settings.DLS_API_KEY,
            )
        else:
            sa_creds_settings = SACredsSettings(
                mode=settings.YC_SA_CREDS_MODE,
                env_key_data=settings.YC_SA_CREDS_KEY_DATA,
            ) if settings.YC_SA_CREDS_MODE is not None else None

            return YCServiceRegistryFactory(
                yc_billing_host=settings.YC_BILLING_HOST if settings.APP_TYPE == AppType.CLOUD else None,
                yc_api_endpoint_rm=settings.YC_RM_CP_ENDPOINT,
                yc_as_endpoint=settings.YC_AUTH_SETTINGS.YC_AS_ENDPOINT if settings.YC_AUTH_SETTINGS else None,
                yc_api_endpoint_iam=settings.YC_AUTH_SETTINGS.YC_API_ENDPOINT_IAM if settings.YC_AUTH_SETTINGS else None,
                yc_ts_endpoint=settings.YC_IAM_TS_ENDPOINT,
                sa_creds_retriever_factory=SACredsRetrieverFactory(
                    sa_creds_settings=sa_creds_settings,
                    ts_endpoint=settings.YC_IAM_TS_ENDPOINT
                ) if sa_creds_settings else None,
                blackbox_name=settings.BLACKBOX_NAME,
            )

    def _get_entity_usage_checker(self, settings: BaseAppSettings) -> Optional[EntityUsageChecker]:
        if settings.APP_TYPE == AppType.CLOUD_PUBLIC:
            return PublicEnvEntityUsageChecker()
        return None

    def _get_bleeding_edge_users(self, settings: BaseAppSettings) -> tuple[str, ...]:
        return settings.BLEEDING_EDGE_USERS if settings.APP_TYPE in (AppType.INTRANET, AppType.TESTS) else tuple()

    def _get_rqe_caches_settings(self, settings: BaseAppSettings) -> Optional[RQECachesSetting]:
        if (
            settings.APP_TYPE in (AppType.CLOUD, AppType.TESTS) and
            isinstance(settings, AsyncAppSettings) and
            settings.RQE_CACHES_ON
        ):
            return RQECachesSetting(
                redis_settings=settings.RQE_CACHES_REDIS,
                caches_ttl=settings.RQE_CACHES_TTL,
            )
        return None

    def _get_default_cache_ttl_settings(self, settings: BaseAppSettings) -> Optional[CacheTTLConfig]:
        if isinstance(settings, AsyncAppSettings) and settings.CACHES_TTL_SETTINGS is not None:
            # FIXME: resolve typing mismatch for CacheTTLConfig
            return CacheTTLConfig(
                ttl_sec_direct=settings.CACHES_TTL_SETTINGS.OTHER,  # type: ignore  # TODO: fix
                ttl_sec_materialized=settings.CACHES_TTL_SETTINGS.MATERIALIZED,  # type: ignore  # TODO: fix
            )
        return None

    def _get_connector_availability(self, settings: BaseAppSettings) -> Optional[ConnectorAvailabilityConfig]:
        return get_connector_availability_config_for_env(settings.ENV_TYPE) if settings.ENV_TYPE is not None else None
        # return settings.CONNECTOR_AVAILABILITY if isinstance(settings, ControlPlaneAppSettings) else None
        # TODO use this^ when contour apps get their configs
