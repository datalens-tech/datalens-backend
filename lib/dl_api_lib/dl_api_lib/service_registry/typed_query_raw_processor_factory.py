from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_core.data_processing.typed_query_raw import CEBasedTypedQueryRawProcessor
from dl_core.us_connection_base import ConnectionBase
from dl_core.utils import FutureRef
from dl_dashsql.typed_query.processor.base import TypedQueryRawProcessorBase
from dl_dashsql.typed_query.processor.cache import (
    CachedTypedQueryProcessor,
    DefaultTypedQueryCacheKeyBuilder,
)


if TYPE_CHECKING:
    from dl_cache_engine.engine import EntityCacheEngineAsync
    from dl_core.services_registry.top_level import ServicesRegistry  # noqa


@attr.s
class TypedQueryRawProcessorFactory(abc.ABC):
    _service_registry_ref: FutureRef["ServicesRegistry"] = attr.ib(kw_only=True)

    @property
    def service_registry(self) -> ServicesRegistry:
        return self._service_registry_ref.ref

    @abc.abstractmethod
    def get_typed_query_processor(
        self,
        connection: ConnectionBase,
        allow_cache_usage: bool = False,
    ) -> TypedQueryRawProcessorBase:
        raise NotImplementedError


class DefaultRawQueryProcessorFactory(TypedQueryRawProcessorFactory):
    def get_typed_query_processor(
        self,
        connection: ConnectionBase,
        allow_cache_usage: bool = False,
    ) -> TypedQueryRawProcessorBase:
        ce_factory = self.service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(conn=connection)
        tq_processor: TypedQueryRawProcessorBase = CEBasedTypedQueryRawProcessor(async_conn_executor=conn_executor)

        # caching is disabled
        # allow_cache_usage param is needed for future possible compatibility
        # TODO: add caching and return cached typed query raw processor?
        # see typed_query_processor_factory.py

        return tq_processor
