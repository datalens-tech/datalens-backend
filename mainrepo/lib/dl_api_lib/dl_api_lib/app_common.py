from __future__ import annotations

import abc
import logging.config
from typing import (
    TYPE_CHECKING,
    Generic,
    Optional,
    TypeVar,
    final,
)

import attr

from dl_api_lib.app_common_settings import ConnOptionsMutatorsFactory
from dl_api_lib.app_settings import AppSettings
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.i18n.registry import (
    LOCALIZATION_CONFIGS,
    register_translation_configs,
)
from dl_api_lib.service_registry.sr_factory import DefaultBiApiSRFactory
from dl_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.enums import RequiredService
from dl_constants.enums import (
    ConnectionType,
    RLSSubjectType,
)
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.mdb_utils import MDBDomainManagerSettings
from dl_core.rls import (
    RLS_FAILED_USER_NAME_PREFIX,
    BaseSubjectResolver,
    RLSSubject,
)
from dl_core.services_registry.entity_checker import EntityUsageChecker
from dl_core.services_registry.file_uploader_client_factory import FileUploaderSettings
from dl_core.services_registry.inst_specific_sr import (
    InstallationSpecificServiceRegistry,
    InstallationSpecificServiceRegistryFactory,
)
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.utils import FutureRef
from dl_i18n.localizer_base import (
    LocalizerLoader,
    TranslationConfig,
)
from dl_task_processor.arq_wrapper import create_arq_redis_settings


if TYPE_CHECKING:
    from dl_core.services_registry.env_manager_factory_base import EnvManagerFactory


LOGGER = logging.getLogger(__name__)


@attr.s
class NotFoundSubjectResolver(BaseSubjectResolver):
    def get_subjects_by_names(self, names: list[str]) -> list[RLSSubject]:
        return [
            RLSSubject(
                subject_id="",
                subject_type=RLSSubjectType.notfound,
                subject_name=RLS_FAILED_USER_NAME_PREFIX + name,
            )
            for name in names
        ]


@attr.s
class StandaloneServiceRegistry(InstallationSpecificServiceRegistry):
    async def get_subject_resolver(self) -> BaseSubjectResolver:
        return NotFoundSubjectResolver()


@attr.s
class StandaloneServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> StandaloneServiceRegistry:
        return StandaloneServiceRegistry(service_registry_ref=sr_ref)


TSettings = TypeVar("TSettings", bound=AppSettings)


class SRFactoryBuilder(Generic[TSettings], abc.ABC):
    @property
    @abc.abstractmethod
    def _is_async_env(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def _get_required_services(self, settings: TSettings) -> set[RequiredService]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_env_manager_factory(self, settings: TSettings) -> EnvManagerFactory:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_inst_specific_sr_factory(
        self,
        settings: TSettings,
    ) -> Optional[InstallationSpecificServiceRegistryFactory]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_entity_usage_checker(self, settings: TSettings) -> Optional[EntityUsageChecker]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_bleeding_edge_users(self, settings: TSettings) -> tuple[str, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_rqe_caches_settings(self, settings: TSettings) -> Optional[RQECachesSetting]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_default_cache_ttl_settings(self, settings: TSettings) -> Optional[CacheTTLConfig]:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_connector_availability(self, settings: TSettings) -> Optional[ConnectorAvailabilityConfig]:
        raise NotImplementedError

    @property
    def _extra_translation_configs(self) -> set[TranslationConfig]:
        return set()

    @final
    def get_sr_factory(
        self,
        settings: TSettings,
        conn_opts_factory: ConnOptionsMutatorsFactory,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> DefaultBiApiSRFactory:
        supported_functions_manager = SupportedFunctionsManager(supported_tags=settings.FORMULA_SUPPORTED_FUNC_TAGS)

        file_uploader_settings = (
            FileUploaderSettings(
                base_url=settings.FILE_UPLOADER_BASE_URL,
                master_token=settings.FILE_UPLOADER_MASTER_TOKEN,
            )
            if settings.FILE_UPLOADER_BASE_URL and settings.FILE_UPLOADER_MASTER_TOKEN
            else None
        )

        mdb_domain_manager_settings = MDBDomainManagerSettings(
            managed_network_enabled=settings.MDB.MANAGED_NETWORK_ENABLED,
            mdb_domains=settings.MDB.DOMAINS,
            mdb_cname_domains=settings.MDB.CNAME_DOMAINS,
            renaming_map=settings.MDB.MANAGED_NETWORK_REMAP,
        )

        required_services: set[RequiredService] = set(self._get_required_services(settings))
        if settings.BI_COMPENG_PG_ON:
            required_services.add(RequiredService.POSTGRES)

        register_translation_configs(self._extra_translation_configs)
        localization_loader = LocalizerLoader(configs=LOCALIZATION_CONFIGS)
        localization_factory = localization_loader.load()
        localizer_fallback = (
            localization_factory.get_for_locale(locale=settings.DEFAULT_LOCALE) if settings.DEFAULT_LOCALE else None
        )

        sr_factory = DefaultBiApiSRFactory(
            async_env=self._is_async_env,
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
