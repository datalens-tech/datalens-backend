from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_core.data_processing.typed_query import CEBasedTypedQueryProcessor
from dl_core.us_connection_base import ConnectionBase
from dl_core.utils import FutureRef
from dl_dashsql.typed_query.processor.base import TypedQueryProcessorBase
from dl_dashsql.typed_query.processor.cache import (
    CachedTypedQueryProcessor,
    DefaultTypedQueryCacheKeyBuilder,
)


if TYPE_CHECKING:
    from dl_cache_engine.engine import EntityCacheEngineAsync
    from dl_core.services_registry.top_level import ServicesRegistry  # noqa


@attr.s
class TypedQueryProcessorFactory(abc.ABC):
    _service_registry_ref: FutureRef["ServicesRegistry"] = attr.ib(kw_only=True)

    @property
    def service_registry(self) -> ServicesRegistry:
        return self._service_registry_ref.ref

    @abc.abstractmethod
    def get_typed_query_processor(
        self,
        connection: ConnectionBase,
        allow_cache_usage: bool = True,
    ) -> TypedQueryProcessorBase:
        raise NotImplementedError


class DefaultQueryProcessorFactory(TypedQueryProcessorFactory):
    def get_typed_query_processor(
        self,
        connection: ConnectionBase,
        allow_cache_usage: bool = True,
    ) -> TypedQueryProcessorBase:
        ce_factory = self.service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(conn=connection)
        tq_processor: TypedQueryProcessorBase = CEBasedTypedQueryProcessor(async_conn_executor=conn_executor)

        allow_cache_usage = allow_cache_usage and connection.allow_cache

        use_cache: bool = False
        cache_engine: Optional[EntityCacheEngineAsync] = None
        if allow_cache_usage and connection.cache_ttl_sec_override:  # (ttl is not None and > 0)
            cache_engine_factory = self.service_registry.get_cache_engine_factory()
            if cache_engine_factory is not None:
                cache_engine = cache_engine_factory.get_cache_engine(entity_id=connection.uuid)
            if cache_engine is not None:
                use_cache = True

        if use_cache:
            assert cache_engine is not None
            tq_processor = CachedTypedQueryProcessor(
                main_processor=tq_processor,
                cache_key_builder=DefaultTypedQueryCacheKeyBuilder(
                    base_key=connection.get_cache_key_part(),
                ),
                cache_engine=cache_engine,
                refresh_ttl_on_read=False,
                cache_ttl_config=self.service_registry.default_cache_ttl_config,
            )

        return tq_processor
