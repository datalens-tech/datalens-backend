from __future__ import annotations

import logging
from typing import (
    Any,
    Optional,
)

import attr

from dl_constants.enums import (
    DataSourceRole,
    ProcessorType,
)
from dl_core.data_processing.processing.cache.processor import CacheOperationProcessor
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor
from dl_core.data_processing.processing.source_db.processor import SourceDbOperationProcessor
from dl_core.services_registry.data_processor_factory_base import BaseClosableDataProcessorFactory
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class SourceDataProcessorFactory(BaseClosableDataProcessorFactory):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def _create_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        reporting_enabled: bool = True,
        # SOURCE_DB-specific
        role: Optional[DataSourceRole] = None,
        **kwargs: Any,
    ) -> ExecutorBasedOperationProcessor:
        assert role is not None
        processor = SourceDbOperationProcessor(
            reporting_registry=self.services_registry.get_reporting_registry(),
            ce_factory=self.services_registry.get_conn_executor_factory(),
            rci=self.services_registry.rci,
            dataset=dataset,
            role=role,
            us_entry_buffer=us_entry_buffer,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
            default_cache_ttl_config=self.services_registry.default_cache_ttl_config,
        )

        if allow_cache_usage:
            cache_engine_factory = self.services_registry.get_cache_engine_factory()
            assert cache_engine_factory is not None
            processor = CacheOperationProcessor(  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "CacheOperationProcessor", variable has type "SourceDbOperationProcessor")  [assignment]
                reporting_registry=self.services_registry.get_reporting_registry(),
                cache_engine_factory=cache_engine_factory,
                dataset_id=dataset.uuid,
                main_processor=processor,
                use_cache=allow_cache_usage,
            )

        return processor


@attr.s(frozen=True)
class CompengDataProcessorFactory(BaseClosableDataProcessorFactory):
    def _create_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        reporting_enabled: bool = True,
        **kwargs: Any,
    ) -> ExecutorBasedOperationProcessor:
        processor: ExecutorBasedOperationProcessor
        dproc_srv_factory = self.services_registry.get_data_processor_service_factory()
        if dproc_srv_factory is None:
            raise ValueError("Processor factory was created without a PG pool. Cannot create a PG processor")

        data_proc_service = dproc_srv_factory(processor_type)
        processor = data_proc_service.get_data_processor(
            reporting_registry=self.services_registry.get_reporting_registry(),
            reporting_enabled=reporting_enabled,
        )

        if allow_cache_usage:
            cache_engine_factory = self.services_registry.get_cache_engine_factory()
            assert cache_engine_factory is not None
            processor = CacheOperationProcessor(
                reporting_registry=self.services_registry.get_reporting_registry(),
                cache_engine_factory=cache_engine_factory,
                dataset_id=dataset.uuid,
                main_processor=processor,
                use_cache=allow_cache_usage,
            )

        return processor


@attr.s(frozen=True)
class DefaultDataProcessorFactory(BaseClosableDataProcessorFactory):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    # internal props
    _source_data_processor_factory: SourceDataProcessorFactory = attr.ib(init=False)
    _compeng_data_processor_factory: CompengDataProcessorFactory = attr.ib(init=False)

    @_source_data_processor_factory.default
    def _make_source_data_processor_factory(self) -> SourceDataProcessorFactory:
        return SourceDataProcessorFactory(
            services_registry_ref=self._services_registry_ref,  # type: ignore  # mypy bug
            is_bleeding_edge_user=self._is_bleeding_edge_user,
        )

    @_compeng_data_processor_factory.default
    def _make_compeng_data_processor_factory(self) -> CompengDataProcessorFactory:
        return CompengDataProcessorFactory(
            services_registry_ref=self._services_registry_ref,  # type: ignore  # mypy bug
        )

    async def get_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        reporting_enabled: bool = True,
        **kwargs: Any,
    ) -> ExecutorBasedOperationProcessor:
        processor: ExecutorBasedOperationProcessor
        if processor_type == ProcessorType.SOURCE_DB:
            processor = await self._source_data_processor_factory.get_data_processor(
                dataset=dataset,
                processor_type=processor_type,
                us_entry_buffer=us_entry_buffer,
                allow_cache_usage=allow_cache_usage,
                reporting_enabled=reporting_enabled,
                **kwargs,
            )
        else:
            processor = await self._compeng_data_processor_factory.get_data_processor(
                dataset=dataset,
                processor_type=processor_type,
                us_entry_buffer=us_entry_buffer,
                allow_cache_usage=allow_cache_usage,
                reporting_enabled=reporting_enabled,
                **kwargs,
            )

        return processor

    async def close_async(self) -> None:
        await self._source_data_processor_factory.close_async()
        await self._compeng_data_processor_factory.close_async()
