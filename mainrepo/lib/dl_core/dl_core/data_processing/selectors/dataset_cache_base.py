from __future__ import annotations

import abc
import logging
from typing import Optional

import attr

from dl_constants.enums import DataSourceRole
from dl_core.data_processing.cache.exc import CachePreparationFailed
from dl_core.data_processing.cache.primitives import LocalKeyRepresentation
from dl_core.data_processing.cache.utils import SelectorCacheOptionsBuilder
from dl_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
from dl_core.data_processing.selectors.base import BIQueryExecutionContext
from dl_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from dl_core.query.bi_query import QueryAndResultInfo


LOGGER = logging.getLogger(__name__)


@attr.s
class DatasetCacheCommonDataSelectorAsyncBase(DatasetDataSelectorAsyncBase, metaclass=abc.ABCMeta):
    """
    Abstract class that implement base cache-related operations:
    - Cache key calculation
    - TTL determination

    If cache usage is allowed in class, it will build cache options during building query execution context.
    """

    _allow_cache_usage: bool = attr.ib(default=True, kw_only=True)
    _is_bleeding_edge_user: bool = attr.ib(default=False)
    _cache_options_builder: SelectorCacheOptionsBuilder = attr.ib(kw_only=True)

    @_cache_options_builder.default  # noqa
    def _cache_options_builder_default(self) -> SelectorCacheOptionsBuilder:
        return SelectorCacheOptionsBuilder(
            default_ttl_config=self._service_registry.default_cache_ttl_config,
            is_bleeding_edge_user=self._is_bleeding_edge_user,
            us_entry_buffer=self._us_entry_buffer,
        )

    def get_data_key(self, query_execution_ctx: BIQueryExecutionContext) -> Optional[LocalKeyRepresentation]:
        cache_options = query_execution_ctx.cache_options
        if cache_options is not None:
            return cache_options.key
        return super().get_data_key(query_execution_ctx=query_execution_ctx)

    def build_query_execution_ctx(  # type: ignore  # TODO: fix
        self,
        *,
        query_id: str,
        query_res_info: QueryAndResultInfo,
        role: DataSourceRole,
        joint_dsrc_info: PreparedMultiFromInfo,
    ) -> BIQueryExecutionContext:
        q_exec_ctx: BIQueryExecutionContext = super().build_query_execution_ctx(
            query_id=query_id,
            query_res_info=query_res_info,
            role=role,
            joint_dsrc_info=joint_dsrc_info,
        )
        if self._allow_cache_usage:
            try:
                cache_options = self._cache_options_builder.get_cache_options(
                    joint_dsrc_info=joint_dsrc_info,
                    role=role,
                    query=q_exec_ctx.query,
                    user_types=q_exec_ctx.requested_bi_types,
                    dataset=self.dataset,
                )

                return attr.evolve(
                    q_exec_ctx,
                    cache_options=cache_options,
                )
            except CachePreparationFailed:
                LOGGER.exception("Cache preparation failed")
                # Do not fail the request though (very likely it will still fail)

        return q_exec_ctx
