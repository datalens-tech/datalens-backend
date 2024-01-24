from __future__ import annotations

from typing import Optional

import attr

from dl_core.data_processing.cache.utils import CacheOptionsBuilderBase
from dl_core.data_processing.processing.cache.exec_adapter import CacheExecAdapter
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase
from dl_core.us_dataset import Dataset


@attr.s
class CacheOperationProcessor(ExecutorBasedOperationProcessor, OperationProcessorAsyncBase):
    _dataset: Dataset = attr.ib(kw_only=True)
    _main_processor: ExecutorBasedOperationProcessor = attr.ib(kw_only=True)
    _use_cache: bool = attr.ib(kw_only=True, default=True)
    _use_locked_cache: bool = attr.ib(kw_only=True, default=True)

    def _make_cache_options_builder(self) -> CacheOptionsBuilderBase:  # type: ignore  # 2024-01-24 # TODO: Return type "CacheOptionsBuilderBase" of "_make_cache_options_builder" incompatible with return type "DatasetOptionsBuilder" in supertype "OperationProcessorAsyncBase"  [override]
        return self._main_processor._cache_options_builder

    def _make_db_ex_adapter(self) -> Optional[ProcessorDbExecAdapterBase]:
        return CacheExecAdapter(
            service_registry=self._service_registry,
            reporting_enabled=self._reporting_enabled,
            cache_options_builder=self._cache_options_builder,
            dataset=self._dataset,
            main_processor=self._main_processor,
            use_cache=self._use_cache,
            use_locked_cache=self._use_locked_cache,
        )

    async def ping(self) -> Optional[int]:
        return await self._main_processor.ping()

    async def start(self) -> None:
        await self._main_processor.start()

    async def end(self) -> None:
        await self._main_processor.end()
