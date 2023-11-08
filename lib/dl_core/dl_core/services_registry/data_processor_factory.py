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
        selector_factory = self.services_registry.get_selector_factory()
        selector = selector_factory.get_dataset_selector(
            dataset=dataset,
            allow_cache_usage=False,  # Use data processor-level cache
            us_entry_buffer=us_entry_buffer,
        )
        processor = SourceDbOperationProcessor(
            service_registry=self.services_registry,
            dataset=dataset,
            selector=selector,
            role=role,
            us_entry_buffer=us_entry_buffer,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
            default_cache_ttl_config=self.services_registry.default_cache_ttl_config,
        )

        if allow_cache_usage:
            processor = CacheOperationProcessor(
                service_registry=self.services_registry,
                dataset=dataset,
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
            service_registry=self.services_registry,
            reporting_enabled=reporting_enabled,
        )

        if allow_cache_usage:
            processor = CacheOperationProcessor(
                service_registry=self.services_registry,
                dataset=dataset,
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
