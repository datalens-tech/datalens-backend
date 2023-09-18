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
    SelectorType,
)
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase
from dl_core.data_processing.processing.processor_dataset_cached import CachedDatasetProcessor
from dl_core.data_processing.processing.source_db.processor import SourceDbOperationProcessor
from dl_core.services_registry.data_processor_factory_base import (
    BaseClosableDataProcessorFactory,
    DataProcessorFactory,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class SourceDataProcessorFactory(BaseClosableDataProcessorFactory):
    def _create_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        # SOURCE_DB-specific
        selector_type: Optional[SelectorType] = None,
        role: Optional[DataSourceRole] = None,
        **kwargs: Any,
    ) -> OperationProcessorAsyncBase:
        assert selector_type is not None
        assert role is not None
        selector_factory = self.services_registry.get_selector_factory()
        selector = selector_factory.get_dataset_selector(
            dataset=dataset,
            selector_type=selector_type,
            allow_cache_usage=allow_cache_usage,
            us_entry_buffer=us_entry_buffer,
        )
        processor = SourceDbOperationProcessor(
            dataset=dataset,
            selector=selector,
            role=role,
            us_entry_buffer=us_entry_buffer,
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
        **kwargs: Any,
    ) -> OperationProcessorAsyncBase:
        processor: OperationProcessorAsyncBase
        dproc_srv_factory = self.services_registry.get_data_processor_service_factory()
        if dproc_srv_factory is None:
            raise ValueError("Processor factory was created without a PG pool. Cannot create a PG processor")

        data_proc_service = dproc_srv_factory(processor_type)
        processor = data_proc_service.get_data_processor()

        if allow_cache_usage:
            processor = CachedDatasetProcessor(
                service_registry=self.services_registry,
                dataset=dataset,
                main_processor=processor,
            )

        return processor


@attr.s(frozen=True)
class DefaultDataProcessorFactory(DataProcessorFactory):
    _source_data_processor_factory: SourceDataProcessorFactory = attr.ib(init=False)
    _compeng_data_processor_factory: CompengDataProcessorFactory = attr.ib(init=False)

    @_source_data_processor_factory.default
    def _make_source_data_processor_factory(self) -> SourceDataProcessorFactory:
        return SourceDataProcessorFactory(
            services_registry_ref=self._services_registry_ref,  # type: ignore  # mypy bug
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
        **kwargs: Any,
    ) -> OperationProcessorAsyncBase:
        processor: OperationProcessorAsyncBase
        if processor_type == ProcessorType.SOURCE_DB:
            processor = await self._source_data_processor_factory.get_data_processor(
                dataset=dataset,
                processor_type=processor_type,
                us_entry_buffer=us_entry_buffer,
                allow_cache_usage=allow_cache_usage,
                **kwargs,
            )
        else:
            processor = await self._compeng_data_processor_factory.get_data_processor(
                dataset=dataset,
                processor_type=processor_type,
                us_entry_buffer=us_entry_buffer,
                allow_cache_usage=allow_cache_usage,
                **kwargs,
            )

        return processor

    async def close_async(self) -> None:
        await self._source_data_processor_factory.close_async()
        await self._compeng_data_processor_factory.close_async()
