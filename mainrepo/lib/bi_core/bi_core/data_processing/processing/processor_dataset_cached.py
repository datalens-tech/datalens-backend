from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Collection, List, Optional

import attr
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.base import PGDialect

from bi_api_commons.reporting.models import DataProcessingCacheInfoReportingRecord

from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
from bi_core.data_processing.cache.processing_helper import CacheProcessingHelper, CacheSituation
from bi_core.data_processing.cache.utils import CacheOptionsBuilderDataProcessor
from bi_core.data_processing.processing.operation import (
    BaseOp, CalcOp, MultiSourceOp, SingleSourceOp,
)
from bi_core.data_processing.processing.processor import SROperationProcessorAsyncBase
from bi_core.data_processing.stream_base import (
    CacheVirtualStream, DataRequestMetaInfo, DataStreamAsync, DataStreamBase,
)
from bi_core.connectors.base.query_compiler import QueryCompiler

if TYPE_CHECKING:
    from bi_core.data_processing.processing.context import OpExecutionContext
    from bi_core.data_processing.processing.processor import OperationProcessorAsyncBase
    from bi_core.data_processing.stream_base import AbstractStream
    from bi_core.data_processing.types import TValuesChunkStream
    from bi_core.us_dataset import Dataset


LOGGER = logging.getLogger(__name__)


@attr.s
class SingleStreamCacheOperationProcessor(SROperationProcessorAsyncBase):
    _dataset: Dataset = attr.ib(kw_only=True)
    _query_compiler: QueryCompiler = attr.ib(kw_only=True)
    _cache_options_builder: CacheOptionsBuilderDataProcessor = attr.ib(kw_only=True)
    _main_processor: OperationProcessorAsyncBase = attr.ib(kw_only=True)

    _cache_helper: Optional[CacheProcessingHelper] = attr.ib(init=False)

    async def ping(self) -> Optional[int]:
        return await self._main_processor.ping()  # TODO: also ping cache engine?

    @_query_compiler.default  # noqa
    def _default_query_compiler(self) -> QueryCompiler:
        return QueryCompiler(dialect=PGDialect())

    @_cache_options_builder.default  # noqa
    def _cache_options_builder_default(self) -> CacheOptionsBuilderDataProcessor:
        return CacheOptionsBuilderDataProcessor(
            default_ttl_config=self._service_registry.default_cache_ttl_config
        )

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        # cache_engine_factory = self.service_registry.get_cache_engine_factory()
        ds_id = self._dataset.uuid
        self._cache_helper = CacheProcessingHelper(
            entity_id=ds_id,  # type: ignore  # TODO: fix
            service_registry=self.service_registry,
        )

    # TODO FIX: @altvod Move data key building to CacheOptionsBuilderDataProcessor
    def _make_op_data_key(self, op: BaseOp, ctx: OpExecutionContext) -> Optional[LocalKeyRepresentation]:
        """Generate new data key for operation"""

        base_key: Optional[LocalKeyRepresentation]
        if isinstance(op, SingleSourceOp):
            # inherit key from single source stream
            stream = ctx.get_stream(op.source_stream_id)
            assert isinstance(stream, DataStreamBase)
            base_key = stream.data_key
        elif isinstance(op, MultiSourceOp):
            # combine key from multiple source streams
            base_key = LocalKeyRepresentation()
            for stream_id in op.source_stream_ids:
                stream = ctx.get_stream(stream_id)
                assert isinstance(stream, DataStreamBase)
                if stream.data_key is None:
                    base_key = None
                    break
                base_key = base_key.multi_extend(*stream.data_key.key_parts)
        else:
            raise TypeError(f'Invalid operation type {type(op)}')

        if base_key is None:
            return None

        data_key = base_key.extend(part_type='operation', part_content=op.__class__.__name__)  # type: ignore  # TODO: fix
        if isinstance(op, CalcOp):
            data_key = data_key.extend(
                part_type='query',
                part_content=CacheOptionsBuilderDataProcessor.get_query_str_for_cache(
                    query=self._query_compiler.compile_select(
                        bi_query=op.bi_query,
                        # The real table name doesn't really matter here
                        sql_source=sa.table('table'),
                    ),
                    dialect=self._query_compiler.dialect,
                )
            )

        return data_key

    async def execute_operation(self, op: BaseOp, ctx: OpExecutionContext) -> CacheVirtualStream:
        """
        Imitate execution by constructing a new virtual stream
        with an updated data key for the cache engine
        """

        if isinstance(op, SingleSourceOp):
            source_stream = ctx.get_stream(op.source_stream_id)
            assert isinstance(source_stream, DataStreamBase)
            user_types = source_stream.user_types
            names = source_stream.names
            stream_meta = source_stream.meta
        elif isinstance(op, MultiSourceOp):
            source_streams = [
                ctx.get_stream(source_stream_id)
                for source_stream_id in sorted(op.source_stream_ids)
            ]
            names = []  # not used
            user_types = []  # not used
            stream_meta = DataRequestMetaInfo(
                query_id=None,  # FIXME
                query=None,
                is_materialized=all(
                    stream.meta.is_materialized
                    for stream in source_streams
                    if isinstance(stream, DataStreamBase)
                ),
                data_source_list=[
                    dsrc
                    for s in source_streams if isinstance(s, DataStreamBase)
                    for dsrc in s.meta.data_source_list
                ]
            )
        else:
            raise TypeError(f'Invalid operation type {type(op)}')

        data_key = self._make_op_data_key(op=op, ctx=ctx)

        return CacheVirtualStream(
            id=op.dest_stream_id,
            names=names,
            user_types=user_types,
            data_key=data_key,
            meta=stream_meta,
        )

    def _save_cache_info_reporting_record(self, ctx: OpExecutionContext, cache_full_hit: bool) -> None:
        report = DataProcessingCacheInfoReportingRecord(
            timestamp=time.time(),
            processing_id=ctx.processing_id,
            cache_full_hit=cache_full_hit,
        )
        self._reporting_registry.save_reporting_record(report=report)

    async def execute_operations(
            self,
            ctx: OpExecutionContext,
            output_stream_ids: Collection[str],
    ) -> List[DataStreamAsync]:
        """
        Turn all virtual output streams into real data-containing ones
        by getting data from the cache or the source.
        """
        if len(output_stream_ids) != 1:
            raise ValueError("This cacher is only applicable for single-output streams")

        original_ctx = ctx.clone()

        # Uses the recursive logic to call `self.execute_operation` which converts
        # the streams into `VirtualStream` objects (and likely mutates the `ctx`).

        # TODO: refactor to use an explicit self-method call instead of super()
        output_streams = await super().execute_operations(ctx=ctx, output_stream_ids=output_stream_ids)
        assert len(output_streams) == 1
        virtual_stream, = output_streams
        assert isinstance(virtual_stream, CacheVirtualStream)
        # Resolve TTL info and save BIQueryCacheOptions object
        cache_options_builder = self._cache_options_builder
        cache_options = cache_options_builder.get_cache_options_for_stream(
            virtual_stream,
            self._dataset
        )

        ds_id = self._dataset.uuid
        cache_helper = CacheProcessingHelper(
            entity_id=ds_id,  # type: ignore  # TODO: fix
            service_registry=self.service_registry,
        )

        async def _get_from_source() -> Optional[TValuesChunkStream]:
            streams = await self._main_processor.execute_operations(
                ctx=original_ctx,
                output_stream_ids=output_stream_ids,
            )
            assert len(streams) == 1
            stream, = streams
            return stream.data

        cache_full_hit = None
        try:
            sit, result_iter = await cache_helper.run_with_cache(
                generate_func=_get_from_source,  # type: ignore  # TODO: fix
                cache_options=cache_options,
            )
            if sit == CacheSituation.full_hit:
                cache_full_hit = True
            elif sit == CacheSituation.generated:
                cache_full_hit = False
        finally:
            self._save_cache_info_reporting_record(
                ctx=ctx,
                cache_full_hit=cache_full_hit,  # type: ignore  # TODO: fix
            )

        result_stream = DataStreamAsync(
            id=virtual_stream.id,
            names=virtual_stream.names,
            user_types=virtual_stream.user_types,
            data=result_iter,  # type: ignore  # TODO: fix
            meta=virtual_stream.meta,
            data_key=cache_options.key,
        )
        return [result_stream]

    def _validate_input_stream(self, stream: AbstractStream, op: BaseOp) -> None:
        if not isinstance(stream, CacheVirtualStream):
            super()._validate_input_stream(stream, op)

    def _validate_output_stream(self, stream: AbstractStream, op: BaseOp) -> None:
        if not isinstance(stream, CacheVirtualStream):
            super()._validate_output_stream(stream, op)


@attr.s
class CachedDatasetProcessor(SROperationProcessorAsyncBase):  # noqa
    _dataset: Dataset = attr.ib(kw_only=True)
    _main_processor: OperationProcessorAsyncBase = attr.ib(kw_only=True)

    _cache_processor: SingleStreamCacheOperationProcessor = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._cache_processor = SingleStreamCacheOperationProcessor(
            dataset=self._dataset,
            service_registry=self.service_registry,
            main_processor=self._main_processor,
        )

    async def ping(self) -> Optional[int]:
        return await self._main_processor.ping()  # TODO: also ping cache engine?

    async def execute_operations(
            self, ctx: OpExecutionContext,
            output_stream_ids: Collection[str],
    ) -> List[DataStreamAsync]:

        if len(output_stream_ids) != 1:
            LOGGER.warning('Cannot currently use cache for multiple (%d) output streams', len(output_stream_ids))
            return await self._main_processor.execute_operations(
                ctx=ctx,
                output_stream_ids=output_stream_ids,
            )

        cache_processor = self._cache_processor
        return await cache_processor.execute_operations(
            ctx=ctx,
            output_stream_ids=output_stream_ids,
        )

    async def start(self) -> None:
        await self._main_processor.start()
        await self._cache_processor.start()

    async def end(self) -> None:
        await self._cache_processor.end()
        await self._main_processor.end()
