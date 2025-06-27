from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_cache_engine.primitives import CacheTTLConfig
from dl_constants.enums import DataSourceRole
from dl_core.data_processing.cache.utils import (
    DatasetOptionsBuilder,
    SelectorCacheOptionsBuilder,
)
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor
from dl_core.data_processing.processing.source_db.selector_exec_adapter import SourceDbExecAdapter
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer


if TYPE_CHECKING:
    from dl_api_commons.base_models import RequestContextInfo
    from dl_core.services_registry.conn_executor_factory_base import ConnExecutorFactory


@attr.s
class SourceDbOperationProcessor(ExecutorBasedOperationProcessor):
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _default_cache_ttl_config: CacheTTLConfig = attr.ib(default=None)
    _ce_factory: ConnExecutorFactory = attr.ib(kw_only=True)
    _rci: RequestContextInfo = attr.ib(kw_only=True)

    def _make_cache_options_builder(self) -> DatasetOptionsBuilder:
        return SelectorCacheOptionsBuilder(
            default_ttl_config=self._default_cache_ttl_config,
        )

    def _make_db_ex_adapter(self) -> Optional[ProcessorDbExecAdapterBase]:
        return SourceDbExecAdapter(
            reporting_registry=self._reporting_registry,
            reporting_enabled=self._reporting_enabled,
            role=self._role,
            dataset=self._dataset,
            row_count_hard_limit=self._row_count_hard_limit,
            us_entry_buffer=self._us_entry_buffer,
            cache_options_builder=self._cache_options_builder,
            ce_factory=self._ce_factory,
            rci=self._rci,
        )
