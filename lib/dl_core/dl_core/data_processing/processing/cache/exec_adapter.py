from __future__ import annotations

import time
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Collection,
    Optional,
    Sequence,
    Union,
)

import attr

from dl_api_commons.reporting.models import DataProcessingCacheInfoReportingRecord
from dl_cache_engine.primitives import LocalKeyRepresentation
from dl_cache_engine.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
)
from dl_constants.enums import (
    JoinType,
    UserDataType,
)
from dl_core.connectors.base.query_compiler import QueryCompiler
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.db_base.processor_base import ExecutorBasedOperationProcessor
from dl_utils.streaming import AsyncChunkedBase


if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import Select

    from dl_core.base_models import ConnectionRef
    from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
    from dl_core.data_processing.types import TValuesChunkStream
    from dl_core.services_registry.cache_engine_factory import CacheEngineFactory


@attr.s
class CacheExecAdapter(ProcessorDbExecAdapterBase):  # noqa
    _dataset_id: Optional[str] = attr.ib(kw_only=True)
    _main_processor: ExecutorBasedOperationProcessor = attr.ib(kw_only=True)
    _use_cache: bool = attr.ib(kw_only=True)
    _use_locked_cache: bool = attr.ib(kw_only=True)
    _cache_engine_factory: CacheEngineFactory = attr.ib(kw_only=True)

    def _save_data_proc_cache_info_reporting_record(self, ctx: OpExecutionContext, cache_full_hit: bool) -> None:
        data_proc_cache_record = DataProcessingCacheInfoReportingRecord(
            timestamp=time.time(),
            processing_id=ctx.processing_id,
            cache_full_hit=cache_full_hit,
        )
        self.add_reporting_record(data_proc_cache_record)

    async def _execute_and_fetch(
        self,
        *,
        query: Union[str, Select],
        user_types: Sequence[UserDataType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedFromInfo] = None,
        query_id: str,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation,
        preparation_callback: Optional[Callable[[], Awaitable[None]]],
    ) -> TValuesChunkStream:
        # Ignore preparation_callback - we do not need to prepare real data here

        # Resolve TTL info and save BIQueryCacheOptions object
        cache_options = self._cache_options_builder.get_cache_options(
            joint_dsrc_info=joint_dsrc_info,  # type: ignore  # 2024-01-24 # TODO: Argument "joint_dsrc_info" to "get_cache_options" of "DatasetOptionsBuilder" has incompatible type "PreparedFromInfo | None"; expected "PreparedFromInfo"  [arg-type]
            data_key=data_key,
        )

        cache_engine = self._cache_engine_factory.get_cache_engine(entity_id=self._dataset_id)
        cache_helper = CacheProcessingHelper(cache_engine=cache_engine)

        original_ctx = ctx.clone()

        async def _get_from_source() -> Optional[TValuesChunkStream]:
            result_data = await self._main_processor.db_ex_adapter.fetch_data_from_select(
                query=query,
                user_types=user_types,
                chunk_size=chunk_size,
                joint_dsrc_info=joint_dsrc_info,
                query_id=query_id,
                ctx=original_ctx,
                data_key=data_key,
                preparation_callback=preparation_callback,
            )
            return result_data

        cache_full_hit: Optional[bool] = None
        try:
            sit, result_iter = await cache_helper.run_with_cache(
                allow_cache_read=self._use_cache,
                generate_func=_get_from_source,  # type: ignore  # TODO: fix
                cache_options=cache_options,
                use_locked_cache=self._use_locked_cache,
            )
            if sit == CacheSituation.full_hit:
                cache_full_hit = True
            elif sit == CacheSituation.generated:
                cache_full_hit = False
        finally:
            self._save_data_proc_cache_info_reporting_record(ctx=ctx, cache_full_hit=cache_full_hit)  # type: ignore  # 2024-01-24 # TODO: Argument "cache_full_hit" to "_save_data_proc_cache_info_reporting_record" of "CacheExecAdapter" has incompatible type "bool | None"; expected "bool"  [arg-type]
            self._main_processor.db_ex_adapter.post_cache_usage(query_id=query_id, cache_full_hit=cache_full_hit)  # type: ignore  # 2024-01-24 # TODO: Argument "cache_full_hit" to "post_cache_usage" of "ProcessorDbExecAdapterBase" has incompatible type "bool | None"; expected "bool"  [arg-type]

        return result_iter  # type: ignore  # 2024-01-24 # TODO: Incompatible return value type (got "AsyncChunkedBase[dict[str, dict[Any, Any] | list[Any] | tuple[Any, ...] | date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None] | list[dict[Any, Any] | list[Any] | tuple[Any, ...] | date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None] | tuple[dict[Any, Any] | list[Any] | tuple[Any, ...] | date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None, ...] | date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None] | None", expected "AsyncChunkedBase[Sequence[date | datetime | time | timedelta | Decimal | UUID | bytes | str | float | int | bool | None]]")  [return-value]

    async def create_table(  # type: ignore  # 2024-01-24 # TODO: Return type "Coroutine[Any, Any, None]" of "create_table" incompatible with return type "Coroutine[Any, Any, TableClause]" in supertype "ProcessorDbExecAdapterBase"  [override]
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
    ) -> None:
        # Proxy the action to main processor
        await self._main_processor.db_ex_adapter.create_table(
            table_name=table_name,
            names=names,
            user_types=user_types,
        )

    async def insert_data_into_table(
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[UserDataType],
        data: AsyncChunkedBase,
    ) -> None:
        # Proxy the action to main processor
        await self._main_processor.db_ex_adapter.insert_data_into_table(
            table_name=table_name,
            names=names,
            user_types=user_types,
            data=data,
        )

    def get_query_compiler(self) -> QueryCompiler:
        return self._main_processor.db_ex_adapter.get_query_compiler()

    def get_supported_join_types(self) -> Collection[JoinType]:
        return self._main_processor.db_ex_adapter.get_supported_join_types()

    def pre_query_execute(
        self,
        query_id: str,
        compiled_query: str,
        target_connection_ref: Optional[ConnectionRef],
    ) -> None:
        self._main_processor.db_ex_adapter.pre_query_execute(
            query_id=query_id,
            compiled_query=compiled_query,
            target_connection_ref=target_connection_ref,
        )

    def post_query_execute(
        self,
        query_id: str,
        exec_exception: Optional[Exception],
    ) -> None:
        self._main_processor.db_ex_adapter.post_query_execute(query_id=query_id, exec_exception=exec_exception)
