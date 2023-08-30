from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING, Any, Callable, ClassVar, Dict,
    FrozenSet, Generic, Optional, Sequence, Type, TypeVar,
)

import attr
from typing_extensions import final
from arq.connections import RedisSettings as ArqRedisSettings

from bi_constants.enums import ProcessorType

from bi_configs.enums import RequiredService

from bi_api_commons.reporting import ReportingRegistry
from bi_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from bi_core.services_registry.selector_factory import DefaultSelectorFactory
from bi_core.services_registry.top_level import DefaultServicesRegistry, ServicesRegistry
from bi_core.services_registry.file_uploader_client_factory import FileUploaderClientFactory, FileUploaderSettings
from bi_core.services_registry.rqe_caches import RQECachesSetting
from bi_core.us_manager.mutation_cache.usentry_mutation_cache_factory import USEntryMutationCacheFactory
from bi_core.utils import FutureRef
from bi_core.mdb_utils import MDBDomainManagerSettings, MDBDomainManagerFactory
from bi_task_processor.processor import ARQTaskProcessorFactory

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from bi_configs.connectors_settings import ConnectorSettingsBase
    from bi_configs.rqe import RQEConfig
    from bi_constants.enums import ConnectionType

    from bi_api_commons.base_models import RequestContextInfo
    from bi_core.data_processing.cache.primitives import CacheTTLConfig
    from bi_core.aio.web_app_services.data_processing.data_processor import DataProcessorService
    from bi_core.services_registry.entity_checker import EntityUsageChecker
    from bi_core.services_registry.env_manager_factory_base import EnvManagerFactory
    from bi_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
    from bi_core.services_registry.typing import ConnectOptionsFactory
    from bi_core.us_connection_base import ExecutorBasedMixin


LOGGER = logging.getLogger(__name__)
SERVICE_REGISTRY_TV = TypeVar('SERVICE_REGISTRY_TV', bound=ServicesRegistry)


class SRFactory(Generic[SERVICE_REGISTRY_TV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_service_registry(
            self,
            request_context_info: RequestContextInfo,
            mutations_redis_client_factory: Optional[Callable[[bool], Optional[Redis]]] = None,
            mutations_cache_factory: Optional[USEntryMutationCacheFactory] = None,
            reporting_registry: Optional[ReportingRegistry] = None,
            caches_redis_client_factory: Optional[Callable[[bool], Optional[Redis]]] = None,
            data_processor_service_factory: Optional[Callable[[ProcessorType], DataProcessorService]] = None,
    ) -> SERVICE_REGISTRY_TV:
        """
        :param request_context_info: RCI to pass to services registry.
        :param caches_redis_client_factory: Callable that will return Redis client for caches Redis.
            Moved to argument because Redis client factory may be instantiated after instantiation of SRFactory.
        :param pg_pool_factory: Factory that produces PG pools (BasePgPoolWrapper subclass instances).
        :param reporting_registry: ReportingRegistry associated with a registry
        :return: New services registry.
        """
        pass


@attr.s
class DefaultSRFactory(SRFactory[SERVICE_REGISTRY_TV]):  # type: ignore  # TODO: fix
    rqe_config: Optional[RQEConfig] = attr.ib()
    async_env: bool = attr.ib()
    env_manager_factory: EnvManagerFactory = attr.ib()
    # Config options
    default_cache_ttl_config: Optional[CacheTTLConfig] = attr.ib(default=None)
    bleeding_edge_users: Sequence[str] = attr.ib(default=())
    conn_cls_whitelist: Optional[FrozenSet[Type[ExecutorBasedMixin]]] = attr.ib(default=None)
    connect_options_factory: Optional[ConnectOptionsFactory] = attr.ib(default=None)
    entity_usage_checker: Optional[EntityUsageChecker] = attr.ib(default=None)
    connectors_settings: dict[ConnectionType, ConnectorSettingsBase] = attr.ib(factory=dict)
    file_uploader_settings: Optional[FileUploaderSettings] = attr.ib(default=None)
    redis_pool_settings: Optional[ArqRedisSettings] = attr.ib(default=None)
    mdb_domain_manager_settings: MDBDomainManagerSettings = attr.ib(factory=MDBDomainManagerSettings)
    force_non_rqe_mode: bool = attr.ib(default=False)
    rqe_caches_settings: Optional[RQECachesSetting] = attr.ib(default=None)
    required_services: set[RequiredService] = attr.ib(factory=set)
    inst_specific_sr_factory: Optional[InstallationSpecificServiceRegistryFactory] = attr.ib(default=None)

    service_registry_cls: ClassVar[Type[SERVICE_REGISTRY_TV]] = DefaultServicesRegistry  # type: ignore  # TODO: fix

    def is_bleeding_edge_user(self, request_context_info: RequestContextInfo) -> bool:
        return request_context_info.user_name in self.bleeding_edge_users

    def make_conn_executor_factory(
            self,
            request_context_info: RequestContextInfo,
            sr_ref: FutureRef[SERVICE_REGISTRY_TV],
    ) -> DefaultConnExecutorFactory:
        is_bleeding_edge_user = self.is_bleeding_edge_user(request_context_info)
        if is_bleeding_edge_user:
            LOGGER.info('ATTENTION! It\'s bleeding edge user')
        return DefaultConnExecutorFactory(
            async_env=self.async_env,
            tpe=None,
            conn_sec_mgr=self.env_manager_factory.make_security_manager(request_context_info),
            mdb_mgr=self.env_manager_factory.make_mdb_domain_manager(self.mdb_domain_manager_settings),
            rqe_config=self.rqe_config,
            services_registry_ref=sr_ref,  # type: ignore  # TODO: fix
            is_bleeding_edge_user=is_bleeding_edge_user,
            conn_cls_whitelist=self.conn_cls_whitelist,
            connect_options_factory=self.connect_options_factory,
            # Env-specific alterations of connect_options:
            connect_options_mutator=self.env_manager_factory.mutate_conn_opts,
            entity_usage_checker=self.entity_usage_checker,
            force_non_rqe_mode=self.force_non_rqe_mode,
        )

    def additional_sr_constructor_kwargs(
            self, request_context_info: RequestContextInfo, sr_ref: FutureRef[ServicesRegistry],
    ) -> Dict[str, Any]:
        return dict()

    @final
    def make_service_registry(
            self,
            request_context_info: RequestContextInfo,
            mutations_redis_client_factory: Optional[Callable[[bool], Optional[Redis]]] = None,
            mutations_cache_factory: Optional[USEntryMutationCacheFactory] = None,
            reporting_registry: Optional[ReportingRegistry] = None,
            # TODO: refactor usage of redis and pg here
            #  (some kind of multi-purpose factory instead of separate getters)
            caches_redis_client_factory: Optional[Callable[[bool], Optional[Redis]]] = None,
            data_processor_service_factory: Optional[Callable[[ProcessorType], DataProcessorService]] = None,
    ) -> SERVICE_REGISTRY_TV:
        sr_ref: FutureRef[ServicesRegistry] = FutureRef()
        sr = self.service_registry_cls(  # type: ignore  # TODO: fix
            default_cache_ttl_config=self.default_cache_ttl_config,
            rci=request_context_info,
            conn_exec_factory=self.make_conn_executor_factory(request_context_info, sr_ref),  # type: ignore  # TODO: fix
            caches_redis_client_factory=caches_redis_client_factory,
            mutations_redis_client_factory=mutations_redis_client_factory,
            mutations_cache_factory=mutations_cache_factory,
            data_processor_service_factory=data_processor_service_factory,
            connectors_settings=self.connectors_settings,
            reporting_registry=reporting_registry,
            selector_factory=DefaultSelectorFactory(
                services_registry_ref=sr_ref,
                is_bleeding_edge_user=self.is_bleeding_edge_user(request_context_info),
            ),
            file_uploader_client_factory=FileUploaderClientFactory(
                self.file_uploader_settings,
            ) if self.file_uploader_settings else None,
            task_processor_factory=ARQTaskProcessorFactory(
                redis_pool_settings=self.redis_pool_settings,
            ) if self.redis_pool_settings else None,
            mdb_domain_manager_factory=MDBDomainManagerFactory(
                settings=self.mdb_domain_manager_settings,
            ) if self.mdb_domain_manager_settings else None,
            rqe_caches_settings=self.rqe_caches_settings,
            required_services=self.required_services,
            inst_specific_sr=(
                self.inst_specific_sr_factory.get_inst_specific_sr(sr_ref)
                if self.inst_specific_sr_factory is not None else None
            ),
            **self.additional_sr_constructor_kwargs(request_context_info, sr_ref),
        )
        sr_ref.fulfill(sr)
        return sr
