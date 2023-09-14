from __future__ import annotations

from functools import wraps
import logging
from typing import (
    Awaitable,
    Callable,
    List,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.selectable import (
    Alias,
    Select,
    Subquery,
)

from bi_core.data_processing.prepared_components.primitives import (
    PreparedFromInfo,
    PreparedMultiFromInfo,
    PreparedSingleFromInfo,
)
from bi_core.data_processing.processing.context import OpExecutionContext
from bi_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from bi_core.data_processing.processing.operation import (
    BaseOp,
    CalcOp,
    DownloadOp,
    JoinOp,
    MultiSourceOp,
    SingleSourceOp,
)
from bi_core.data_processing.source_builder import SqlSourceBuilder
from bi_core.data_processing.stream_base import (
    AbstractStream,
    DataRequestMetaInfo,
    DataSourceVS,
    DataStreamAsync,
    JointDataSourceVS,
)
from bi_core.utils import (
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
    async def execute(self, op: BaseOp) -> DataStreamAsync:  # type: ignore  # TODO: fix
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

        query_debug_str = compile_query_for_debug(query=query, dialect=query_compiler.dialect)
        LOGGER.info(f"Going to database with SQL query:\n{query_debug_str}")

        joint_dsrc_info = PreparedMultiFromInfo(
            sql_source=query,
            data_source_list=source_stream.prep_src_info.data_source_list,  # type: ignore  # TODO: fix
            db_name=source_stream.prep_src_info.db_name,
            connect_args=source_stream.prep_src_info.connect_args,
            query_compiler=source_stream.prep_src_info.query_compiler,
            supported_join_types=source_stream.prep_src_info.supported_join_types,
            pass_db_query_to_user=source_stream.prep_src_info.pass_db_query_to_user,
            target_connection_ref=source_stream.prep_src_info.target_connection_ref,
        )  # FIXME: replace usage with prep_src_info

        query_id = make_id()
        data = await self.db_ex_adapter.fetch_data_from_select(
            query=query,
            user_types=source_stream.user_types,
            joint_dsrc_info=joint_dsrc_info,
            query_id=query_id,
        )

        if op.row_count_hard_limit is not None:
            data = data.limit(op.row_count_hard_limit)

        data_key = self.db_ex_adapter.get_data_key(
            query=query, user_types=source_stream.user_types, joint_dsrc_info=joint_dsrc_info, query_id=query_id
        )

        pass_db_query_to_user = joint_dsrc_info.pass_db_query_to_user
        data_source_list = source_stream.prep_src_info.data_source_list or ()
        assert data_source_list is not None

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

    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> DataSourceVS:  # type: ignore  # TODO: fix
        assert isinstance(op, CalcOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)

        from_info: PreparedFromInfo
        if isinstance(source_stream, DataSourceVS):
            from_info = source_stream.prep_src_info
        elif isinstance(source_stream, JointDataSourceVS):
            from_info = source_stream.joint_dsrc_info
        else:
            raise TypeError(f"Type {type(source_stream).__name__} is not supported as a source for CalcOp")

        query_compiler = from_info.query_compiler
        query = query_compiler.compile_select(
            bi_query=op.bi_query,
            sql_source=from_info.sql_source,
        )
        query_debug_str = compile_query_for_debug(query=query, dialect=query_compiler.dialect)

        LOGGER.info(f"Generated lazy query: {query_debug_str}")

        names = []
        user_types = []
        for expr_ctx in op.bi_query.select_expressions:
            assert expr_ctx.alias is not None
            assert expr_ctx.user_type is not None
            names.append(expr_ctx.alias)
            user_types.append(expr_ctx.user_type)

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
        )
        return DataSourceVS(
            id=op.dest_stream_id,
            alias=op.alias,
            result_id=op.result_id,
            prep_src_info=prep_src_info,
            names=prep_src_info.col_names,
            user_types=user_types,
        )


class JoinOpExecutorAsync(OpExecutorAsync):
    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> JointDataSourceVS:  # type: ignore  # TODO: fix
        assert isinstance(op, JoinOp)

        prepared_sources: List[PreparedSingleFromInfo] = []
        for stream_id in op.source_stream_ids:
            stream = self.ctx.get_stream(stream_id=stream_id)
            assert isinstance(stream, DataSourceVS)
            prepared_sources.append(stream.prep_src_info)

        source_builder = SqlSourceBuilder()

        joint_dsrc_info = source_builder.build_source(
            root_avatar_id=op.root_avatar_id,
            prepared_sources=prepared_sources,
            join_on_expressions=op.join_on_expressions,
            use_empty_source=op.use_empty_source,
        )
        return JointDataSourceVS(
            id=op.dest_stream_id, names=[], user_types=[], joint_dsrc_info=joint_dsrc_info  # not used  # not used
        )
