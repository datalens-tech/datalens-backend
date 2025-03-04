from __future__ import annotations

from functools import wraps
import logging
from typing import (
    Awaitable,
    Callable,
)

import attr
import shortuuid
import sqlalchemy as sa
from sqlalchemy.sql.selectable import (
    Alias,
    Select,
    Subquery,
)

from dl_cache_engine.primitives import LocalKeyRepresentation
from dl_core.data_processing.prepared_components.primitives import (
    PreparedFromInfo,
    PreparedSingleFromInfo,
)
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from dl_core.data_processing.processing.operation import (
    BaseOp,
    CalcOp,
    DownloadOp,
    JoinOp,
    MultiSourceOp,
    SingleSourceOp,
    UploadOp,
)
from dl_core.data_processing.source_builder import SqlSourceBuilder
from dl_core.data_processing.stream_base import (
    AbstractStream,
    AsyncVirtualStream,
    DataRequestMetaInfo,
    DataSourceVS,
    DataStreamAsync,
    JointDataSourceVS,
)
import dl_core.exc as exc
from dl_core.utils import (
    compile_query_for_debug,
    make_id,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class OpExecutorAsync:
    ctx: OpExecutionContext = attr.ib()
    db_ex_adapter: ProcessorDbExecAdapterBase = attr.ib()

    async def execute(self, op: BaseOp) -> AbstractStream:
        raise NotImplementedError


def log_op(
    func: Callable[[OpExecutorAsync, BaseOp], Awaitable[AbstractStream]]
) -> Callable[[OpExecutorAsync, BaseOp], Awaitable[AbstractStream]]:
    """Log operation calls"""

    @wraps(func)
    async def wrapper(self: OpExecutorAsync, op: BaseOp) -> AbstractStream:
        assert isinstance(op, (SingleSourceOp, MultiSourceOp))
        stream_ids = [op.source_stream_id] if isinstance(op, SingleSourceOp) else op.source_stream_ids
        LOGGER.info(
            f"Running {type(op).__name__} "
            f"with source streams {stream_ids} "
            f"and destination stream {op.dest_stream_id}"
        )
        return await func(self, op)

    return wrapper


class DownloadOpExecutorAsync(OpExecutorAsync):
    """Loads data from a database table"""

    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> DataStreamAsync:
        assert isinstance(op, DownloadOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)
        assert isinstance(source_stream, DataSourceVS)

        query_compiler = source_stream.prep_src_info.query_compiler
        query = source_stream.prep_src_info.sql_source
        if isinstance(query, Alias):
            # Unwrap Alias:
            # 1. There is no reason for alias when executing directly.
            # 2. Serialization/deserialization of Alias is not supported for RQE
            query = query.original

        if isinstance(query, Subquery):
            # Unwrap Subquery for sake of sa 1.3 compatibility with 1.4.
            # TODO: Confirm that it is ok or adapt Subquery feature for our code.
            query = query.original

        assert isinstance(query, Select), f"Got type {type(query).__name__} for query, expected Select"
        joint_dsrc_info = source_stream.prep_src_info.clone(sql_source=query)

        query_debug_str = compile_query_for_debug(query=query, dialect=query_compiler.dialect)
        LOGGER.info(f"Going to database with SQL query:\n{query_debug_str}")

        query_id = make_id()

        self.db_ex_adapter.pre_query_execute(
            query_id=query_id,
            compiled_query=query_debug_str,
            target_connection_ref=joint_dsrc_info.target_connection_ref,
        )

        data = await self.db_ex_adapter.fetch_data_from_select(
            query=query,
            user_types=source_stream.user_types,
            joint_dsrc_info=joint_dsrc_info,
            query_id=query_id,
            ctx=self.ctx,
            data_key=source_stream.data_key,
            preparation_callback=source_stream.prepare,
        )

        if op.row_count_hard_limit is not None:
            data = data.limit(
                max_count=op.row_count_hard_limit,
                limit_exception=exc.ResultRowCountLimitExceeded,
            )

        self.db_ex_adapter.post_query_execute(
            query_id=query_id,
            exec_exception=None,  # FIXME
        )

        pass_db_query_to_user = joint_dsrc_info.pass_db_query_to_user
        data_source_list = source_stream.prep_src_info.data_source_list or ()
        assert data_source_list is not None

        data_key = source_stream.data_key
        return DataStreamAsync(
            id=op.dest_stream_id,
            names=source_stream.names,
            user_types=source_stream.user_types,
            data=data,
            meta=DataRequestMetaInfo(
                query=query_debug_str,
                data_source_list=data_source_list,
                query_id=query_id,
                pass_db_query_to_user=pass_db_query_to_user,
            ),
            data_key=data_key,
        )


class CalcOpExecutorAsync(OpExecutorAsync):
    """
    Applies query to stream (lazy operation).
    The real calculation is done when the data is fetched.
    """

    def get_from_info_from_stream(self, source_stream: AbstractStream) -> PreparedFromInfo:
        from_info: PreparedFromInfo
        if isinstance(source_stream, DataSourceVS):
            from_info = source_stream.prep_src_info
        elif isinstance(source_stream, JointDataSourceVS):
            from_info = source_stream.joint_dsrc_info
        else:
            raise TypeError(f"Type {type(source_stream).__name__} is not supported as a source for CalcOp")
        return from_info

    def make_data_key(self, op: BaseOp) -> LocalKeyRepresentation:
        assert isinstance(op, CalcOp)
        source_stream = self.ctx.get_stream(op.source_stream_id)

        # TODO: Remove legacy version

        # Legacy procedure
        from_info = self.get_from_info_from_stream(source_stream=source_stream)  # type: ignore  # 2024-01-24 # TODO: Argument "source_stream" to "get_from_info_from_stream" of "CalcOpExecutorAsync" has incompatible type "AbstractStream | None"; expected "AbstractStream"  [arg-type]
        query_compiler = from_info.query_compiler
        query = query_compiler.compile_select(
            bi_query=op.bi_query,
            # The info about the real source is already contained in the previous key parts,
            # and, also, we want to avoid the randomized table names (in compeng) to appear in the key.
            # So just use a fake table here.
            sql_source=sa.table("table"),
        )
        legacy_data_key = self.db_ex_adapter.get_data_key(
            query=query,
            user_types=source_stream.user_types,  # type: ignore  # 2024-01-24 # TODO: Item "None" of "AbstractStream | None" has no attribute "user_types"  [union-attr]
            from_info=from_info,
            base_key=source_stream.data_key,  # type: ignore  # 2024-01-24 # TODO: Item "None" of "AbstractStream | None" has no attribute "data_key"  [union-attr]
        )

        # New procedure
        new_data_key = source_stream.data_key.extend("query", op.data_key_data)  # type: ignore  # 2024-01-24 # TODO: Item "None" of "AbstractStream | None" has no attribute "data_key"  [union-attr]

        LOGGER.info(
            f"Preliminary cache key info for query: "
            f"legacy key: {legacy_data_key.key_parts_hash} ; "  # type: ignore  # 2024-01-24 # TODO: Item "None" of "LocalKeyRepresentation | None" has no attribute "key_parts_hash"  [union-attr]
            f"new key: {new_data_key.key_parts_hash}"
        )

        return new_data_key

    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> DataSourceVS:
        assert isinstance(op, CalcOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)
        assert isinstance(source_stream, AsyncVirtualStream)

        from_info = self.get_from_info_from_stream(source_stream=source_stream)
        query_compiler = from_info.query_compiler
        query = query_compiler.compile_select(
            bi_query=op.bi_query,
            sql_source=from_info.sql_source,
        )
        query_debug_str = compile_query_for_debug(query=query, dialect=query_compiler.dialect)

        LOGGER.info(f"Generated lazy query: {query_debug_str}")

        names = op.bi_query.get_names()
        user_types = op.bi_query.get_user_types()

        data_key = self.make_data_key(op=op)

        alias = op.alias
        prep_src_info = PreparedSingleFromInfo(
            id=op.result_id,
            alias=alias,
            col_names=names,
            user_types=user_types,
            sql_source=sa.alias(query, alias),
            query_compiler=query_compiler,
            data_source_list=from_info.data_source_list,
            supported_join_types=from_info.supported_join_types,
            db_name=from_info.db_name,
            connect_args=from_info.connect_args,
            pass_db_query_to_user=from_info.pass_db_query_to_user,
            target_connection_ref=from_info.target_connection_ref,
            data_key=data_key,
        )

        return DataSourceVS(
            id=op.dest_stream_id,
            alias=op.alias,
            result_id=op.result_id,
            prep_src_info=prep_src_info,
            names=prep_src_info.col_names,
            user_types=user_types,
            data_key=data_key,
            meta=DataRequestMetaInfo(data_source_list=prep_src_info.data_source_list),  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "DataRequestMetaInfo" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
            preparation_callback=source_stream.prepare,
        )


class JoinOpExecutorAsync(OpExecutorAsync):
    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> JointDataSourceVS:
        assert isinstance(op, JoinOp)

        prepared_sources: list[PreparedSingleFromInfo] = []
        callbacks: list[Callable[[], Awaitable[None]]] = []
        for stream_id in op.source_stream_ids:
            stream = self.ctx.get_stream(stream_id=stream_id)
            assert isinstance(stream, DataSourceVS)
            prepared_sources.append(stream.prep_src_info)
            callbacks.append(stream.prepare)

        async def joint_preparation_callback() -> None:
            # Can't perform them simultaneously with one connection,
            # so iterate and run one by one
            for cb in callbacks:
                await cb()

        source_builder = SqlSourceBuilder()

        joint_dsrc_info = source_builder.build_source(
            root_avatar_id=op.root_avatar_id,
            prepared_sources=prepared_sources,
            join_on_expressions=op.join_on_expressions,
            use_empty_source=op.use_empty_source,
        )
        data_key = joint_dsrc_info.data_key
        return JointDataSourceVS(
            id=op.dest_stream_id,
            names=[],
            user_types=[],
            joint_dsrc_info=joint_dsrc_info,  # not used  # not used
            meta=DataRequestMetaInfo(
                data_source_list=joint_dsrc_info.data_source_list,  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "DataRequestMetaInfo" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
            ),
            data_key=data_key,
            preparation_callback=joint_preparation_callback,
        )


class UploadOpExecutorAsync(OpExecutorAsync):
    """Dumps incoming stream to a database table"""

    def make_data_key(self, op: BaseOp) -> LocalKeyRepresentation:
        assert isinstance(op, UploadOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)
        return source_stream.data_key  # type: ignore  # 2024-01-24 # TODO: Item "None" of "AbstractStream | None" has no attribute "data_key"  [union-attr]

    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> DataSourceVS:
        assert isinstance(op, UploadOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)
        assert isinstance(source_stream, DataStreamAsync)

        table_name = shortuuid.uuid().lower()
        executed = False

        async def upload_data() -> None:
            nonlocal executed

            if executed:
                LOGGER.info(f"Skipping upload to table {table_name} as it was already executed")
                return

            LOGGER.info(f"Uploading to table {table_name}")
            await self.db_ex_adapter.create_table(
                table_name=table_name,
                names=source_stream.names,
                user_types=source_stream.user_types,
            )
            await self.db_ex_adapter.insert_data_into_table(
                table_name=table_name,
                names=source_stream.names,
                user_types=source_stream.user_types,
                data=source_stream.data,
            )
            executed = True

        alias = op.alias
        sql_source = sa.alias(sa.table(table_name), alias)

        data_key = self.make_data_key(op=op)

        prep_src_info = PreparedSingleFromInfo(
            id=op.result_id,
            alias=alias,
            query_compiler=self.db_ex_adapter.get_query_compiler(),
            sql_source=sql_source,
            col_names=source_stream.names,
            user_types=source_stream.user_types,
            data_source_list=tuple(source_stream.meta.data_source_list),
            supported_join_types=self.db_ex_adapter.get_supported_join_types(),
            db_name=None,
            connect_args={},
            pass_db_query_to_user=False,
            target_connection_ref=None,
            data_key=data_key,
        )

        return DataSourceVS(
            id=op.dest_stream_id,
            result_id=prep_src_info.id,
            prep_src_info=prep_src_info,
            names=prep_src_info.col_names,
            user_types=prep_src_info.user_types,
            alias=op.alias,
            data_key=data_key,
            meta=DataRequestMetaInfo(data_source_list=prep_src_info.data_source_list),  # type: ignore  # 2024-01-24 # TODO: Argument "data_source_list" to "DataRequestMetaInfo" has incompatible type "tuple[DataSource, ...] | None"; expected "Collection[DataSource]"  [arg-type]
            preparation_callback=upload_data,
        )
