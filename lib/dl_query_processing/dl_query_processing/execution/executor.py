from __future__ import annotations

import asyncio
import logging
from typing import (
    AbstractSet,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
)

import attr

from dl_constants.enums import (
    DataSourceRole,
    ProcessorType,
)
from dl_constants.types import TBIDataRow
from dl_core.components.ids import AvatarId
from dl_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from dl_core.data_processing.processing.operation import (
    BaseOp,
    CalcOp,
    DownloadOp,
    JoinOp,
    UploadOp,
)
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase
from dl_core.data_processing.stream_base import (
    AbstractStream,
    DataRequestMetaInfo,
    DataSourceVS,
    DataStreamAsync,
)
from dl_core.services_registry import ServicesRegistry
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.utils import make_id
from dl_query_processing import exc
from dl_query_processing.enums import (
    EmptyQueryMode,
    ExecutionLevel,
    QueryType,
)
from dl_query_processing.execution.exec_info import QueryExecutionInfo
from dl_query_processing.execution.executor_base import QueryExecutorBase
from dl_query_processing.execution.primitives import (
    ExecutedQuery,
    ExecutedQueryMetaInfo,
)
from dl_query_processing.translation.primitives import TranslatedMultiQueryBase


LOGGER = logging.getLogger(__name__)


@attr.s
class SourceDbBaseSourceInfo:
    root_avatar_id: AvatarId = attr.ib(kw_only=True)
    required_avatar_ids: Collection[AvatarId] = attr.ib(kw_only=True)


@attr.s
class QueryExecutor(QueryExecutorBase):
    _dataset: Dataset = attr.ib(kw_only=True)
    _compeng_processor_type: ProcessorType = attr.ib(kw_only=True)
    _source_db_processor_type: ProcessorType = attr.ib(kw_only=True)
    _allow_cache_usage: bool = attr.ib(kw_only=True)
    _avatar_alias_mapper: Optional[Callable[[str], str]] = attr.ib(kw_only=True, default=None)  # noqa
    _us_manager: USManagerBase = attr.ib(kw_only=True)
    _compeng_semaphore: asyncio.Semaphore = attr.ib(kw_only=True)

    @property
    def _service_registry(self) -> ServicesRegistry:
        return self._us_manager.get_services_registry()

    async def _get_compeng_data_processor(self) -> OperationProcessorAsyncBase:
        factory = self._service_registry.get_data_processor_factory()
        return await factory.get_data_processor(
            dataset=self._dataset,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
            processor_type=self._compeng_processor_type,
            allow_cache_usage=self._allow_cache_usage,
        )

    async def _get_source_db_data_processor(self, role: DataSourceRole) -> OperationProcessorAsyncBase:
        factory = self._service_registry.get_data_processor_factory()
        return await factory.get_data_processor(
            dataset=self._dataset,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
            processor_type=self._source_db_processor_type,
            allow_cache_usage=self._allow_cache_usage,
            role=role,
        )

    async def _get_data_processor(
        self,
        level_type: ExecutionLevel,
        role: DataSourceRole,
    ) -> OperationProcessorAsyncBase:
        if level_type == ExecutionLevel.source_db:
            return await self._get_source_db_data_processor(role=role)
        if level_type == ExecutionLevel.compeng:
            return await self._get_compeng_data_processor()
        raise ValueError(f"Unsupported level_type: {level_type}")

    async def _process_multi_query(
        self,
        *,
        level_type: ExecutionLevel,
        streams_by_result_id: Dict[AvatarId, AbstractStream],
        translated_multi_query: TranslatedMultiQueryBase,
        stream_aliases: Dict[str, str],
        upload_inputs: bool,
        distinct: bool,
        role: DataSourceRole,
        row_count_hard_limit: int,
    ) -> Tuple[Dict[AvatarId, DataStreamAsync], Dict[AvatarId, str]]:
        if translated_multi_query.is_empty():
            for stream in streams_by_result_id.values():
                assert isinstance(stream, DataStreamAsync)
            return streams_by_result_id, stream_aliases  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "tuple[dict[str, AbstractStream], dict[str, str]]", expected "tuple[dict[str, DataStreamAsync], dict[str, str]]")  [return-value]

        op: BaseOp  # For usage in various parts of this function

        LOGGER.info(
            f"Executing level type {level_type}. " f"Got source streams with result IDs: {list(streams_by_result_id)}"
        )

        streams: List[DataStreamAsync] = [stream for _, stream in sorted(streams_by_result_id.items())]  # type: ignore  # TODO: fix
        operations: List[BaseOp] = []

        # 1. Make operations for uploading data (if necessary).
        # For every incoming data stream
        if upload_inputs:
            result_to_stream_id_map: Dict[AvatarId, str] = {}
            for result_id, stream in sorted(streams_by_result_id.items()):
                op = UploadOp(
                    result_id=result_id,
                    source_stream_id=stream.id,
                    alias=stream_aliases[stream.id],
                    dest_stream_id=make_id(),
                )
                operations.append(op)
                result_to_stream_id_map[result_id] = op.dest_stream_id
        else:
            result_to_stream_id_map = {result_id: stream.id for result_id, stream in streams_by_result_id.items()}

        required_avatar_ids: AbstractSet[str]

        # 2. Make operations for queries.
        # These can have many levels
        all_query_ids: set[str] = set()
        selected_avatar_ids: set[str] = set()  # avatar IDs used by at least one query
        for translated_flat_query in translated_multi_query.iter_queries():
            all_query_ids.add(translated_flat_query.id)
            selected_avatar_ids |= set(translated_flat_query.joined_from.iter_ids())
            dest_stream_id = make_id()
            # Save ID relationship for future resolution when creating CalcOps
            result_to_stream_id_map[translated_flat_query.id] = dest_stream_id

        top_level_query_ids = all_query_ids - selected_avatar_ids  # IDs of top-level queries (not in any FROMs)

        top_calc_stream_to_result_and_alias_map: Dict[str, Tuple[AvatarId, str]] = {}
        for translated_flat_query in translated_multi_query.iter_queries():
            query_froms = translated_flat_query.joined_from.froms
            is_top_level = translated_flat_query.id in top_level_query_ids
            is_bottom_level = bool(query_froms) and not any(from_obj.id in all_query_ids for from_obj in query_froms)
            if translated_flat_query.is_empty():
                raise exc.EmptyQuery()
            result_id = translated_flat_query.id
            required_avatar_ids = frozenset(translated_flat_query.joined_from.iter_ids())

            is_bottom_source_db = is_bottom_level and level_type == ExecutionLevel.source_db
            is_empty_source = len(required_avatar_ids) == 0
            is_multi_source = len(required_avatar_ids) > 1

            if is_bottom_source_db or is_empty_source or is_multi_source:
                # All of these cases require a `JoinOp`:
                # 1. JoinOp is required before the first-level query in source_db.
                #    The SELECT is made directly from the inter-joined source avatars.
                # 2. SELECT containing only constants or data-independent functions (see below)
                # 3. Multiple FROMs - real JOIN is required here

                root_avatar_id: Optional[str] = translated_flat_query.joined_from.root_from_id

                if is_empty_source:
                    # FIXME: This is quite a dirty hack and should be refactored in the long term
                    # This is a special case when the query selects only constants,
                    # such as with zero-dimensional LODs,
                    # and is used a a sort of glue that binds these LOD sub-queries.
                    # In this case the query must return exactly one line
                    # (it should not have a FROM clause), and yet it still needs
                    # a data source for all the meta stuff
                    # (connection instance, SQL compiler, etc.).
                    # For this we use the `use_empty_source` flag to indicate that,
                    # even though we have passed a list of sources to the JoinOp,
                    # the real FROM clause should be empty.

                    root_avatar_id = next(iter(translated_multi_query.get_base_root_from_ids()))
                    required_avatar_ids = {root_avatar_id}

                assert root_avatar_id is not None
                source_stream_ids = {result_to_stream_id_map[str_res_id] for str_res_id in required_avatar_ids}
                op = JoinOp(
                    source_stream_ids=source_stream_ids,
                    dest_stream_id=make_id(),
                    join_on_expressions=translated_flat_query.join_on,
                    root_avatar_id=root_avatar_id,
                    use_empty_source=is_empty_source,
                )
                operations.append(op)
                calc_op_source_stream_id = op.dest_stream_id

            else:
                # Query requires a single source
                source_result_id = next(iter(required_avatar_ids))
                calc_op_source_stream_id = result_to_stream_id_map[source_result_id]

            bi_query = self.make_bi_query(
                translated_flat_query=translated_flat_query,
                is_top_level=is_top_level,
                distinct=distinct,
                row_count_hard_limit=row_count_hard_limit,
            )
            op = CalcOp(
                result_id=result_id,
                source_stream_id=calc_op_source_stream_id,
                alias=translated_flat_query.alias,
                bi_query=bi_query,
                dest_stream_id=result_to_stream_id_map[result_id],
                data_key_data=translated_flat_query.extract,
            )
            assert isinstance(op, CalcOp)  # for typing
            operations.append(op)
            # Save stream IDs for top-level queries
            if is_top_level:
                top_calc_stream_to_result_and_alias_map[op.dest_stream_id] = (op.result_id, op.alias)

        # 3. Operations for downloading data from the top-level queries
        output_stream_to_result_map: Dict[str, AvatarId] = {}
        output_stream_aliases: Dict[str, str] = {}
        for top_calc_stream_id, (result_id, alias) in top_calc_stream_to_result_and_alias_map.items():
            op = DownloadOp(
                source_stream_id=top_calc_stream_id,
                dest_stream_id=make_id(),
                row_count_hard_limit=row_count_hard_limit,
            )
            assert isinstance(op, DownloadOp)  # for typing
            LOGGER.info(
                f"Adding DownloadOp for query {result_id} with "
                f"input stream {op.source_stream_id} and output stream {op.dest_stream_id}"
            )
            operations.append(op)
            output_stream_to_result_map[op.dest_stream_id] = result_id
            output_stream_aliases[op.dest_stream_id] = alias

        processor = await self._get_data_processor(level_type=level_type, role=role)

        output_stream_list = await processor.run(
            operations=operations,
            streams=streams,
            output_stream_ids=set(output_stream_to_result_map),
        )
        output_streams_by_result_id = {output_stream_to_result_map[stream.id]: stream for stream in output_stream_list}
        return output_streams_by_result_id, output_stream_aliases

    def _make_source_db_input_streams(
        self,
        *,
        role: DataSourceRole,
        translated_multi_query: TranslatedMultiQueryBase,
        from_subquery: bool,
        subquery_limit: Optional[int],
    ) -> Tuple[Dict[AvatarId, AbstractStream], Dict[AvatarId, str]]:
        base_avatar_ids = translated_multi_query.get_base_root_from_ids()
        required_avatar_ids: list[str] = [from_id for from_id in translated_multi_query.get_base_froms()]
        required_avatar_ids = sorted(set(required_avatar_ids + list(base_avatar_ids)))

        prep_component_manager = DefaultPreparedComponentManager(
            dataset=self._dataset,
            role=role,
            us_entry_buffer=self._us_manager.get_entry_buffer(),
        )

        streams_by_result_id: Dict[AvatarId, AbstractStream] = {}
        stream_aliases: dict[str, str] = {}
        for avatar_id in required_avatar_ids:
            alias = self._avatar_alias_mapper(avatar_id)  # type: ignore  # TODO: fix
            prep_src_info = prep_component_manager.get_prepared_source(
                avatar_id=avatar_id,
                alias=alias,
                from_subquery=from_subquery,
                subquery_limit=subquery_limit,
            )
            result_id = avatar_id

            stream = DataSourceVS(
                id=make_id(),
                alias=alias,
                result_id=result_id,
                names=prep_src_info.col_names,
                user_types=prep_src_info.user_types,
                prep_src_info=prep_src_info,
                data_key=prep_src_info.data_key,
                meta=DataRequestMetaInfo(data_source_list=prep_src_info.data_source_list),  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "DataRequestMetaInfo" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
                preparation_callback=None,
            )
            streams_by_result_id[result_id] = stream
            stream_aliases[stream.id] = stream.alias

        id_map_for_log = {result_id: stream.id for result_id, stream in streams_by_result_id.items()}
        LOGGER.info(f"Generated base streams for result IDs: {id_map_for_log}")
        LOGGER.info(f"Using aliases: {stream_aliases}")
        assert len(streams_by_result_id) == len(stream_aliases)
        return streams_by_result_id, stream_aliases

    async def execute_query(self, exec_info: QueryExecutionInfo) -> ExecutedQuery:
        translated_multi_query: TranslatedMultiQueryBase = exec_info.translated_multi_query

        top_queries = translated_multi_query.get_top_queries()
        assert len(top_queries) == 1
        top_query = top_queries[0]

        distinct = top_query.meta.query_type == QueryType.distinct
        empty_query_mode = top_query.meta.empty_query_mode

        source_db_multi_query = translated_multi_query.for_level_type(ExecutionLevel.source_db)
        compeng_multi_query = translated_multi_query.for_level_type(ExecutionLevel.compeng)

        compeng_is_top = not compeng_multi_query.is_empty()

        from_subquery = top_query.meta.from_subquery  # FIXME
        subquery_limit = top_query.meta.subquery_limit  # FIXME
        row_count_hard_limit = top_query.meta.row_count_hard_limit

        streams_by_result_id, stream_aliases = self._make_source_db_input_streams(
            role=exec_info.role,
            translated_multi_query=translated_multi_query,
            from_subquery=from_subquery,
            subquery_limit=subquery_limit,
        )

        # Process data in source_db
        try:
            streams_by_result_id, stream_aliases = await self._process_multi_query(  # type: ignore  # TODO: fix
                level_type=ExecutionLevel.source_db,
                translated_multi_query=source_db_multi_query,
                streams_by_result_id=streams_by_result_id,
                stream_aliases=stream_aliases,
                distinct=False if compeng_is_top else distinct,
                upload_inputs=False,
                role=exec_info.role,
                row_count_hard_limit=row_count_hard_limit,
            )
        except exc.EmptyQuery as e:
            if empty_query_mode == EmptyQueryMode.error:
                raise
            empty_rows: List[TBIDataRow]
            if empty_query_mode == EmptyQueryMode.empty:
                empty_rows = []
            elif empty_query_mode == EmptyQueryMode.empty_row:
                empty_rows = [[]]
            else:
                raise ValueError(empty_query_mode) from e
            return ExecutedQuery(
                rows=empty_rows,
                meta=ExecutedQueryMetaInfo.from_trans_meta(
                    trans_meta=top_query.meta,
                    debug_query=None,
                ),
            )

        source_db_queries = [
            out_s.meta.query
            for _, out_s in sorted(streams_by_result_id.items())
            if out_s.meta.query is not None and out_s.meta.pass_db_query_to_user
        ]
        query_for_response = "\n;\n\n".join(source_db_queries) or None

        # Process data in compeng
        async with self._compeng_semaphore:
            streams_by_result_id, stream_aliases = await self._process_multi_query(  # type: ignore  # TODO: fix
                level_type=ExecutionLevel.compeng,
                translated_multi_query=compeng_multi_query,
                streams_by_result_id=streams_by_result_id,
                stream_aliases=stream_aliases,
                distinct=distinct if compeng_is_top else False,
                upload_inputs=True,
                role=exec_info.role,
                row_count_hard_limit=row_count_hard_limit,
            )

        streams = [stream for stream in streams_by_result_id.values()]
        assert len(streams) == 1, f"There must be exactly one output data stream, got {len(streams)}"
        one_stream = streams[0]

        assert isinstance(one_stream, DataStreamAsync)
        rows = await one_stream.data.all()

        executed_query = ExecutedQuery(
            rows=rows,
            meta=ExecutedQueryMetaInfo.from_trans_meta(
                trans_meta=top_query.meta,
                debug_query=query_for_response,
                target_connection_ids=set(
                    target_conn.uuid for target_conn in exec_info.target_connections if target_conn.uuid is not None
                ),
            ),
        )
        return executed_query
