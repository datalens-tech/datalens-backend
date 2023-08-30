from __future__ import annotations

import attr
import shortuuid

import sqlalchemy as sa

from bi_constants.enums import JoinType

from bi_core.data_processing.prepared_components.primitives import PreparedSingleFromInfo
from bi_core.data_processing.stream_base import DataStreamAsync, DataSourceVS
from bi_core.data_processing.processing.operation import BaseOp, UploadOp
from bi_core.data_processing.processing.db_base.op_executors import OpExecutorAsync, log_op
from bi_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from bi_core.connectors.base.query_compiler import QueryCompiler


COMPENG_SUPPORTED_JOIN_TYPES = frozenset({
    JoinType.inner,
    JoinType.left,
    JoinType.right,
    JoinType.full,
})


@attr.s
class PgOpExecutorAsync(OpExecutorAsync):
    @property
    def pgex_adapter(self) -> PostgreSQLExecAdapterAsync:
        assert isinstance(self.db_ex_adapter, PostgreSQLExecAdapterAsync)
        return self.db_ex_adapter


class UploadOpExecutorAsync(PgOpExecutorAsync):
    """Dumps incoming stream to a database table"""

    @log_op  # type: ignore  # TODO: fix
    async def execute(self, op: BaseOp) -> DataSourceVS:  # type: ignore  # TODO: fix
        assert isinstance(op, UploadOp)

        source_stream = self.ctx.get_stream(op.source_stream_id)
        assert isinstance(source_stream, DataStreamAsync)

        table_name = shortuuid.uuid()
        await self.pgex_adapter.create_table(
            table_name=table_name, names=source_stream.names,
            user_types=source_stream.user_types,
        )
        await self.pgex_adapter.insert_data_into_table(
            table_name=table_name, names=source_stream.names,
            user_types=source_stream.user_types, data=source_stream.data,
        )

        alias = op.alias
        sql_source = sa.alias(sa.table(table_name), alias)

        prep_src_info = PreparedSingleFromInfo(
            id=op.result_id,
            alias=alias,
            query_compiler=QueryCompiler(dialect=self.pgex_adapter.dialect),
            sql_source=sql_source,
            col_names=source_stream.names,
            user_types=source_stream.user_types,
            data_source_list=None,
            supported_join_types=COMPENG_SUPPORTED_JOIN_TYPES,
            db_name=None,
            connect_args={},
            pass_db_query_to_user=False,
            target_connection_ref=None,
        )

        return DataSourceVS(
            id=op.dest_stream_id,
            result_id=prep_src_info.id,
            prep_src_info=prep_src_info,
            names=prep_src_info.col_names,
            user_types=prep_src_info.user_types,
            alias=op.alias,
        )
