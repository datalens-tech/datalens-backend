from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    Type,
    TypeVar,
)

import attr
import redis.asyncio

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.reporting.registry import ReportingRegistry
from dl_cache_engine.primitives import CacheTTLConfig
from dl_configs.enums import RequiredService
from dl_constants.enums import ProcessorType
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.services_registry.cache_engine_factory import (
    CacheEngineFactory,
    DefaultCacheEngineFactory,
)
from dl_core.services_registry.compute_executor import ComputeExecutorTPE
from dl_core.services_registry.conn_executor_factory_base import ConnExecutorFactory
from dl_core.services_registry.data_processor_factory import DefaultDataProcessorFactory
from dl_core.services_registry.data_processor_factory_base import BaseClosableDataProcessorFactory
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistry
from dl_core.services_registry.rqe_caches import RQECachesSetting
from dl_core.us_manager.mutation_cache.engine_factory import (
    DefaultMutationCacheEngineFactory,
    MutationCacheEngineFactory,
)
from dl_core.us_manager.mutation_cache.usentry_mutation_cache import GenericCacheEngine
from dl_core.us_manager.mutation_cache.usentry_mutation_cache_factory import USEntryMutationCacheFactory
from dl_core.utils import FutureRef
from dl_task_processor.processor import TaskProcessorFactory


if TYPE_CHECKING:
    from dl_configs.connectors_settings import ConnectorSettingsBase
    from dl_constants.enums import ConnectionType
    from dl_core.aio.web_app_services.data_processing.data_processor import DataProcessorService
    from dl_core.services_registry.compute_executor import ComputeExecutor
    from dl_core.services_registry.file_uploader_client_factory import FileUploaderClientFactory
    from dl_core.us_manager.local_cache import USEntryBuffer


_ISSR_T = TypeVar("_ISSR_T", bound=InstallationSpecificServiceRegistry)


class ServicesRegistry(metaclass=abc.ABCMeta):
    """
    Composition of services/service factories that are used in US entries methods:
     * Connection executors
     * Cache engines
     * ...etc
    """

    @property
    @abc.abstractmethod
    def rci(self) -> RequestContextInfo:
        pass

    @property
    @abc.abstractmethod
    def default_cache_ttl_config(self) -> CacheTTLConfig:
        pass

    @abc.abstractmethod
    def get_conn_executor_factory(self) -> ConnExecutorFactory:
        pass

    @abc.abstractmethod
    def get_caches_redis_client(self, allow_slave: bool = False) -> Optional[redis.asyncio.Redis]:
        pass

    @abc.abstractmethod
    def get_mutations_redis_client(self, allow_slave: bool = False) -> Optional[redis.asyncio.Redis]:
        pass

    @abc.abstractmethod
    def get_mutation_cache_factory(self) -> Optional[USEntryMutationCacheFactory]:
        pass

    @abc.abstractmethod
    def get_reporting_registry(self) -> ReportingRegistry:
        pass

    @abc.abstractmethod
    def get_compute_executor(self) -> ComputeExecutor:
        pass

    @abc.abstractmethod
    def get_data_processor_service_factory(self) -> Optional[Callable[[ProcessorType], DataProcessorService]]:
        pass

    @abc.abstractmethod
    def get_data_processor_factory(self) -> BaseClosableDataProcessorFactory:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    async def close_async(self) -> None:
        pass

    @abc.abstractmethod
    def get_cache_engine_factory(self, allow_slave: bool = False) -> Optional[CacheEngineFactory]:
        pass

    @abc.abstractmethod
    def get_mutation_cache_engine_factory(
        self, cache_type: Type[GenericCacheEngine]
    ) -> MutationCacheEngineFactory:  # type: ignore  # TODO: fix
        pass

    @abc.abstractmethod
    def get_connectors_settings(self, conn_type: ConnectionType) -> Optional[ConnectorSettingsBase]:
        pass

    @abc.abstractmethod
    def get_data_source_collection_factory(self, us_entry_buffer: USEntryBuffer) -> DataSourceCollectionFactory:
        pass

    @abc.abstractmethod
    def get_file_uploader_client_factory(self) -> FileUploaderClientFactory:
        pass

    @abc.abstractmethod
    def get_task_processor_factory(self) -> TaskProcessorFactory:
        pass

    @abc.abstractmethod
    def get_rqe_caches_settings(self) -> Optional[RQECachesSetting]:
        pass

    @abc.abstractmethod
    def get_required_services(self) -> set[RequiredService]:
        pass

    @abc.abstractmethod
    def get_installation_specific_service_registry(self, issr_cls: Type[_ISSR_T]) -> _ISSR_T:
        pass


@attr.s(hash=False)
class DefaultServicesRegistry(ServicesRegistry):  # type: ignore  # TODO: fix
    _rci: RequestContextInfo = attr.ib()
    _reporting_registry: ReportingRegistry = attr.ib()
    _mutations_cache_factory: Optional[USEntryMutationCacheFactory] = attr.ib()
    _mutations_redis_client_factory: Optional[Callable[[bool], Optional[redis.asyncio.Redis]]] = attr.ib(default=None)
    _default_cache_ttl_config: Optional[CacheTTLConfig] = attr.ib(default=None)
    _conn_exec_factory: Optional[ConnExecutorFactory] = attr.ib(default=None)
    _caches_redis_client_factory: Optional[Callable[[bool], Optional[redis.asyncio.Redis]]] = attr.ib(default=None)
    _compute_executor: ComputeExecutor = attr.ib()
    _cache_engine_factory: CacheEngineFactory = attr.ib()
    _mutation_cache_engine_factory: MutationCacheEngineFactory = attr.ib(default=None)
    _data_processor_service_factory: Optional[Callable[[ProcessorType], DataProcessorService]] = attr.ib(default=None)
    _data_processor_factory: BaseClosableDataProcessorFactory = attr.ib()
    _connectors_settings: dict[ConnectionType, ConnectorSettingsBase] = attr.ib(default=None)
    _file_uploader_client_factory: Optional[FileUploaderClientFactory] = attr.ib(default=None)
    _task_processor_factory: Optional[TaskProcessorFactory] = attr.ib(default=None)
    _rqe_caches_settings: Optional[RQECachesSetting] = attr.ib(default=None)
    _required_services: set[RequiredService] = attr.ib(factory=set)
    _inst_specific_sr: Optional[InstallationSpecificServiceRegistry] = attr.ib(default=None)

    @_compute_executor.default  # noqa
    def _default_compute_executor(self) -> ComputeExecutor:
        return ComputeExecutorTPE()  # type: ignore  # Incompatible return value type (got "ComputeExecutorTPE", expected "ComputeExecutor")

    @_cache_engine_factory.default  # noqa
    def _default_cache_engine_factory(self) -> CacheEngineFactory:
        return DefaultCacheEngineFactory(services_registry_ref=FutureRef.fulfilled(self))

    @_data_processor_factory.default  # noqa
    def _default_data_processor_factory(self) -> BaseClosableDataProcessorFactory:
        return DefaultDataProcessorFactory(  # type: ignore  # TODO: fix
            services_registry_ref=FutureRef.fulfilled(self),
        )

    @property
    def rci(self) -> RequestContextInfo:
        return self._rci

    @property
    def default_cache_ttl_config(self) -> CacheTTLConfig:
        if self._default_cache_ttl_config is None:
            self._default_cache_ttl_config = CacheTTLConfig()
        return self._default_cache_ttl_config

    def get_conn_executor_factory(self) -> ConnExecutorFactory:
        if self._conn_exec_factory is None:
            raise ValueError("ConnExecutor factory was not injected in this service registry")
        return self._conn_exec_factory

    def get_caches_redis_client(self, allow_slave: bool = False) -> Optional[redis.asyncio.Redis]:
        if self._caches_redis_client_factory is not None:
            return self._caches_redis_client_factory(allow_slave)
        return None

    def get_mutations_redis_client(self, allow_slave: bool = False) -> Optional[redis.asyncio.Redis]:
        if self._mutations_redis_client_factory is not None:
            return self._mutations_redis_client_factory(allow_slave)
        return None

    def get_reporting_registry(self) -> ReportingRegistry:
        return self._reporting_registry

    def get_compute_executor(self) -> ComputeExecutor:
        return self._compute_executor

    def get_cache_engine_factory(self) -> Optional[CacheEngineFactory]:  # type: ignore  # TODO: fix
        return self._cache_engine_factory

    def get_mutation_cache_factory(self) -> Optional[USEntryMutationCacheFactory]:
        return self._mutations_cache_factory

    def get_mutation_cache_engine_factory(
        self, cache_type: Type[GenericCacheEngine]
    ) -> MutationCacheEngineFactory:  # type: ignore  # TODO: fix
        # TODO: Save already created CacheEngine's?
        return DefaultMutationCacheEngineFactory(services_registry_ref=FutureRef.fulfilled(self), cache_type=cache_type)

    def get_data_processor_service_factory(self) -> Optional[Callable[[ProcessorType], DataProcessorService]]:
        return self._data_processor_service_factory

    def get_data_processor_factory(self) -> BaseClosableDataProcessorFactory:
        return self._data_processor_factory

    def get_rqe_caches_settings(self) -> Optional[RQECachesSetting]:
        return self._rqe_caches_settings

    def get_required_services(self) -> set[RequiredService]:
        return self._required_services

    def get_connectors_settings(self, conn_type: ConnectionType) -> Optional[ConnectorSettingsBase]:
        return self._connectors_settings.get(conn_type)

    def get_data_source_collection_factory(self, us_entry_buffer: USEntryBuffer) -> DataSourceCollectionFactory:
        return DataSourceCollectionFactory(us_entry_buffer=us_entry_buffer)

    def get_file_uploader_client_factory(self) -> FileUploaderClientFactory:
        if self._file_uploader_client_factory is not None:
            return self._file_uploader_client_factory
        raise ValueError("FileUploaderClientFactory hasn't been initialized.")

    def get_task_processor_factory(self) -> TaskProcessorFactory:
        if self._task_processor_factory is not None:
            return self._task_processor_factory
        raise ValueError("TaskProcessorFactory hasn't been initialized.")

    def get_installation_specific_service_registry(self, issr_cls: Type[_ISSR_T]) -> _ISSR_T:
        if isinstance(self._inst_specific_sr, issr_cls):
            return self._inst_specific_sr
        raise ValueError(f"Invalid ISST type: expected {issr_cls}, got {type(self._inst_specific_sr)}")

    def close(self) -> None:
        if self._conn_exec_factory is not None:
            self._conn_exec_factory.close_sync()
        # FIXME: others? (TPE if necessary)
        if self._compute_executor is not None:
            self._compute_executor.close()
        if self._task_processor_factory is not None:
            self._task_processor_factory.cleanup()

    async def close_async(self) -> None:
        if self._data_processor_factory is not None:
            # Processor factory must be closed before selectors
            # because processors might use selectors while closing
            await self._data_processor_factory.close_async()
        if self._conn_exec_factory is not None:
            await self._conn_exec_factory.close_async()
        if self._task_processor_factory is not None:
            await self._task_processor_factory.cleanup_async()

    def clone(self, **kwargs):  # type: ignore  # TODO: fix
        return attr.evolve(self, **kwargs)


@attr.s
class DummyServiceRegistry(ServicesRegistry):
    """
    Service registry implementation that holds RCI for simple test cases (actual services are not required)
    """

    _rci: RequestContextInfo = attr.ib()
    _default_cache_ttl_config: Optional[CacheTTLConfig] = attr.ib(default=None)
    _inst_specific_sr: Optional[InstallationSpecificServiceRegistry] = attr.ib(default=None)

    NOT_IMPLEMENTED_MSG = "DummyServiceRegistry acts only as RCI container for underlying services"

    @property
    def rci(self) -> RequestContextInfo:
        return self._rci

    @property
    def default_cache_ttl_config(self) -> CacheTTLConfig:
        if self._default_cache_ttl_config is None:
            self._default_cache_ttl_config = CacheTTLConfig()
        return self._default_cache_ttl_config

    def get_conn_executor_factory(self) -> ConnExecutorFactory:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_caches_redis_client(self) -> Optional[redis.asyncio.Redis]:  # type: ignore  # TODO: fix
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_mutations_redis_client(self) -> Optional[redis.asyncio.Redis]:  # type: ignore  # TODO: fix
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_reporting_registry(self) -> ReportingRegistry:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_compute_executor(self) -> ComputeExecutor:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_cache_engine_factory(self) -> Optional[CacheEngineFactory]:  # type: ignore  # TODO: fix
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_mutation_cache_factory(self) -> Optional[USEntryMutationCacheFactory]:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_mutation_cache_engine_factory(
        self, cache_type: Type[GenericCacheEngine]
    ) -> MutationCacheEngineFactory:  # type: ignore  # TODO: fix
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_data_processor_service_factory(self) -> Optional[Callable[[ProcessorType], DataProcessorService]]:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_data_processor_factory(self) -> BaseClosableDataProcessorFactory:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_connectors_settings(self, conn_type: ConnectionType) -> Optional[ConnectorSettingsBase]:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_data_source_collection_factory(self, us_entry_buffer: USEntryBuffer) -> DataSourceCollectionFactory:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_file_uploader_client_factory(self) -> FileUploaderClientFactory:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_task_processor_factory(self) -> TaskProcessorFactory:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_rqe_caches_settings(self) -> Optional[RQECachesSetting]:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_required_services(self) -> set[RequiredService]:
        raise NotImplementedError(self.NOT_IMPLEMENTED_MSG)

    def get_installation_specific_service_registry(self, issr_cls: Type[_ISSR_T]) -> _ISSR_T:
        if isinstance(self._inst_specific_sr, issr_cls):
            return self._inst_specific_sr
        raise ValueError(f"Invalid ISST type: expected {issr_cls}, got {type(self._inst_specific_sr)}")

    def close(self) -> None:
        pass

    async def close_async(self) -> None:
        pass
