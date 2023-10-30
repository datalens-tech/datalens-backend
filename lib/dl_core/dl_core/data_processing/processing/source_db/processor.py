from __future__ import annotations

from typing import Optional

import attr

from dl_constants.enums import DataSourceRole
from dl_core.data_processing.cache.primitives import CacheTTLConfig
from dl_core.data_processing.cache.utils import (
    DatasetOptionsBuilder,
    SelectorCacheOptionsBuilder,
)
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor
from dl_core.data_processing.processing.source_db.selector_exec_adapter import SourceDbExecAdapter
from dl_core.data_processing.selectors.base import DataSelectorAsyncBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer


@attr.s
class SourceDbOperationProcessor(ExecutorBasedOperationProcessor):
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _selector: DataSelectorAsyncBase = attr.ib(kw_only=True)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _is_bleeding_edge_user: bool = attr.ib(default=False)
    _default_cache_ttl_config: CacheTTLConfig = attr.ib(default=None)

    def _make_cache_options_builder(self) -> DatasetOptionsBuilder:
        return SelectorCacheOptionsBuilder(
            default_ttl_config=self._default_cache_ttl_config,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
            us_entry_buffer=self._us_entry_buffer,
        )

    def _make_db_ex_adapter(self) -> Optional[ProcessorDbExecAdapterBase]:
        return SourceDbExecAdapter(
            service_registry=self.service_registry,
            reporting_enabled=self._reporting_enabled,
            role=self._role,
            dataset=self._dataset,
            selector=self._selector,
            row_count_hard_limit=self._row_count_hard_limit,
            us_entry_buffer=self._us_entry_buffer,
            cache_options_builder=self._cache_options_builder,
        )
