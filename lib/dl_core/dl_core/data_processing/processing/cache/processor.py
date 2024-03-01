from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_core.data_processing.cache.utils import CacheOptionsBuilderBase
from dl_core.data_processing.processing.cache.exec_adapter import CacheExecAdapter
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor


if TYPE_CHECKING:
    from dl_core.services_registry.cache_engine_factory import CacheEngineFactory


@attr.s
class CacheOperationProcessor(ExecutorBasedOperationProcessor):
    _dataset_id: Optional[str] = attr.ib(kw_only=True)
    _main_processor: ExecutorBasedOperationProcessor = attr.ib(kw_only=True)
    _use_cache: bool = attr.ib(kw_only=True, default=True)
    _use_locked_cache: bool = attr.ib(kw_only=True, default=True)
    _cache_engine_factory: CacheEngineFactory = attr.ib(kw_only=True)

    def _make_cache_options_builder(self) -> CacheOptionsBuilderBase:  # type: ignore  # 2024-01-24 # TODO: Return type "CacheOptionsBuilderBase" of "_make_cache_options_builder" incompatible with return type "DatasetOptionsBuilder" in supertype "OperationProcessorAsyncBase"  [override]
        return self._main_processor._cache_options_builder

    def _make_db_ex_adapter(self) -> Optional[ProcessorDbExecAdapterBase]:
        return CacheExecAdapter(
            reporting_registry=self._reporting_registry,
            reporting_enabled=self._reporting_enabled,
            cache_options_builder=self._cache_options_builder,
            dataset_id=self._dataset_id,
            main_processor=self._main_processor,
            use_cache=self._use_cache,
            use_locked_cache=self._use_locked_cache,
            cache_engine_factory=self._cache_engine_factory,
        )

    async def ping(self) -> Optional[int]:
        return await self._main_processor.ping()

    async def start(self) -> None:
        await self._main_processor.start()

    async def end(self) -> None:
        await self._main_processor.end()
