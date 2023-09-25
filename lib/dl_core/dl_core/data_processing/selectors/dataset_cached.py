from __future__ import annotations

import logging
import time
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_commons.reporting.models import QueryExecutionCacheInfoReportingRecord
from dl_constants.enums import DataSourceRole
from dl_core.data_processing.cache.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
)
from dl_core.data_processing.selectors.base import BIQueryExecutionContext
from dl_core.data_processing.selectors.dataset_cache_base import DatasetCacheCommonDataSelectorAsyncBase
from dl_core.data_processing.selectors.db import DatasetDbDataSelectorAsync


if TYPE_CHECKING:
    from dl_core.data_processing.types import TValuesChunkStream
    from dl_core.us_connection_base import ExecutorBasedMixin

LOGGER = logging.getLogger(__name__)


@attr.s
class CachedDatasetDataSelectorAsync(DatasetCacheCommonDataSelectorAsyncBase):
    """
    Asynchronous cached dataset data selector

    :param allow_cache_usage: if ``True``, cache **might** be used. Otherwise,
    neither read nor write will be executed.
    """

    _allow_cache_usage: bool = attr.ib(default=True, kw_only=True)
    # cache-wrapped selector
    _db_selector: DatasetDbDataSelectorAsync = attr.ib(init=False)
    _cache_helper: Optional[CacheProcessingHelper] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self._db_selector = DatasetDbDataSelectorAsync(
            dataset=self.dataset,
            service_registry=self._service_registry,
            us_entry_buffer=self._us_entry_buffer,
        )
        ds_id = self.dataset.uuid
        if ds_id:
            self._cache_helper = CacheProcessingHelper(
                entity_id=ds_id,
                service_registry=self.service_registry,
            )

    async def execute_query_context(
        self,
        role: DataSourceRole,
        query_execution_ctx: BIQueryExecutionContext,
        row_count_hard_limit: Optional[int] = None,
    ) -> Optional[TValuesChunkStream]:
        async def _request_db() -> Optional[TValuesChunkStream]:
            result_iter = await self._db_selector.execute_query_context(
                role=role, query_execution_ctx=query_execution_ctx, row_count_hard_limit=row_count_hard_limit
            )

            if result_iter is None:
                return None

            # Is this still necessary, after passing `row_count_hard_limit` to the `db_selector`?
            if row_count_hard_limit is not None:
                result_iter = result_iter.limit(max_count=row_count_hard_limit)

            return result_iter

        cache_helper = self._cache_helper
        cache_options = query_execution_ctx.cache_options
        if cache_helper is None or cache_options is None:
            # cache not applicable, call db selector directly
            return await _request_db()

        target_connection: ExecutorBasedMixin = query_execution_ctx.target_connection
        # TODO: make this env-configurable through settings.
        use_locked_cache = target_connection.use_locked_cache

        cache_full_hit = None
        try:
            sit, result_iter = await cache_helper.run_with_cache(
                generate_func=_request_db,  # type: ignore  # TODO: fix
                cache_options=cache_options,
                use_locked_cache=use_locked_cache,
            )
            if sit == CacheSituation.full_hit:
                cache_full_hit = True
            elif sit == CacheSituation.generated:
                cache_full_hit = False
        finally:
            query_cache_rr = QueryExecutionCacheInfoReportingRecord(
                query_id=query_execution_ctx.query_id,
                cache_full_hit=cache_full_hit,
                timestamp=time.time(),
            )
            self.reporting_registry.save_reporting_record(query_cache_rr)

        return result_iter  # type: ignore  # TODO: fix
