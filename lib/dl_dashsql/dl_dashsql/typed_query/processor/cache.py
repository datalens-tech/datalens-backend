import abc
from typing import (
    Iterable,
    Optional,
    Sequence,
)

import attr

from dl_cache_engine.engine import EntityCacheEngineAsync
from dl_cache_engine.primitives import (
    BIQueryCacheOptions,
    CacheTTLConfig,
    LocalKeyRepresentation,
)
from dl_cache_engine.processing_helper import CacheProcessingHelper
from dl_constants.types import TJSONExt
from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryResult,
)
from dl_dashsql.typed_query.processor.base import TypedQueryProcessorBase
from dl_dashsql.typed_query.query_serialization import get_typed_query_serializer
from dl_dashsql.typed_query.result_serialization import get_typed_query_result_serializer
from dl_utils.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
)


class TypedQueryCacheKeyBuilderBase(abc.ABC):
    @abc.abstractmethod
    def get_cache_key(self, typed_query: TypedQuery) -> LocalKeyRepresentation:
        raise NotImplementedError


@attr.s
class DefaultTypedQueryCacheKeyBuilder(TypedQueryCacheKeyBuilderBase):
    base_key: LocalKeyRepresentation = attr.ib(kw_only=True)

    def get_cache_key(self, typed_query: TypedQuery) -> LocalKeyRepresentation:
        tq_serializer = get_typed_query_serializer(query_type=typed_query.query_type)
        serialized_query = tq_serializer.serialize(typed_query)
        local_key_rep = self.base_key.extend(part_type="typed_query_data", part_content=serialized_query)
        return local_key_rep


@attr.s
class CachedTypedQueryProcessor(TypedQueryProcessorBase):
    _main_processor: TypedQueryProcessorBase = attr.ib(kw_only=True)
    _cache_engine: EntityCacheEngineAsync = attr.ib(kw_only=True)
    _cache_ttl_config: CacheTTLConfig = attr.ib(kw_only=True)
    _refresh_ttl_on_read: bool = attr.ib(kw_only=True)
    _cache_key_builder: TypedQueryCacheKeyBuilderBase = attr.ib(kw_only=True)

    def get_cache_options(self, typed_query: TypedQuery) -> BIQueryCacheOptions:
        local_key_rep = self._cache_key_builder.get_cache_key(typed_query=typed_query)
        cache_options = BIQueryCacheOptions(
            cache_enabled=True,
            ttl_sec=self._cache_ttl_config.ttl_sec_direct,
            key=local_key_rep,
            refresh_ttl_on_read=self._refresh_ttl_on_read,
        )
        return cache_options

    async def process_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        tq_result_serializer = get_typed_query_result_serializer(query_type=typed_query.query_type)

        async def generate_func() -> Optional[AsyncChunkedBase[TJSONExt]]:
            source_typed_query_result = await self._main_processor.process_typed_query(typed_query=typed_query)
            # Pack everything into a single string value and wrap that into a stream
            dumped_serialized_data = tq_result_serializer.serialize(source_typed_query_result)
            # 1 chunk of length 1, containing a row with 1 item
            chunked_data: Iterable[Sequence[list[TJSONExt]]] = [[[dumped_serialized_data]]]
            return AsyncChunked.from_chunked_iterable(chunked_data)

        cache_helper = CacheProcessingHelper(cache_engine=self._cache_engine)
        cache_options = self.get_cache_options(typed_query=typed_query)
        cache_situation, chunked_stream = await cache_helper.run_with_cache(
            generate_func=generate_func,
            cache_options=cache_options,
            allow_cache_read=True,
            use_locked_cache=False,
        )

        # TODO: Some logging? For instance, log `cache_situation`

        assert chunked_stream is not None
        data_rows = await chunked_stream.all()

        # Extract the serialized data
        assert len(data_rows) == 1
        first_row = data_rows[0]
        assert isinstance(first_row, (list, tuple)) and len(first_row) == 1
        loaded_serialized_data = first_row[0]
        assert isinstance(loaded_serialized_data, str)

        typed_query_result = tq_result_serializer.deserialize(loaded_serialized_data)
        return typed_query_result
