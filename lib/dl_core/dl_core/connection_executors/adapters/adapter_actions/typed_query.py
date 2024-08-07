from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

import attr
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncTypedQueryAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.sync_base import SyncTypedQueryAdapterAction
from dl_core.connection_executors.adapters.adapters_base import (
    DBAdapterQueryResult,
    SyncDirectDBAdapter,
)
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStepCursorInfo,
)
from dl_core.exc import WrongQueryParameterization
from dl_dashsql.formatting.base import (
    QueryFormatterFactory,
    QueryIncomingParameter,
)
from dl_dashsql.literalizer import DashSQLParamLiteralizer
from dl_dashsql.typed_query.primitives import (
    PlainTypedQuery,
    TypedQuery,
    TypedQueryResult,
    TypedQueryResultColumnHeader,
)
from dl_type_transformer.exc import UnsupportedNativeTypeError
from dl_type_transformer.native_type import GenericNativeType
from dl_type_transformer.type_transformer import TypeTransformer


if TYPE_CHECKING:
    from dl_core.connection_executors.adapters.async_adapters_base import AsyncDBAdapter


@attr.s(frozen=True)
class AsyncTypedQueryAdapterActionEmptyDataRows(AsyncTypedQueryAdapterAction):
    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        return TypedQueryResult(
            query_type=typed_query.query_type,
            column_headers=(),
            data_rows=(),
        )


@attr.s(frozen=True)
class TypedQueryToDBAQueryConverter:
    """
    Converts TypedQuery instance to DBAdapterQuery instance
    for compatibility with the execute adapter method.
    """

    _query_formatter_factory: QueryFormatterFactory = attr.ib(kw_only=True)
    _literalizer: DashSQLParamLiteralizer = attr.ib(kw_only=True)

    def _make_sa_query(self, typed_query: TypedQuery) -> ClauseElement:
        assert isinstance(typed_query, PlainTypedQuery)  # the only type of query we know how to deal with here

        # Perform parameter formatting
        query_formatter = self._query_formatter_factory.get_query_formatter()
        formatter_incoming_parameters = [
            QueryIncomingParameter(
                original_name=param.name,
                user_type=param.typed_value.type,
                value=param.typed_value.value,
            )
            for param in typed_query.parameters
        ]
        formatting_result = query_formatter.format_query(
            query=typed_query.query,
            incoming_parameters=formatter_incoming_parameters,
        )
        formatted_query = formatting_result.formatted_query

        # Convert to SA
        try:
            sa_text = sa.text(formatted_query.query).bindparams(
                *[
                    sa.bindparam(
                        param.param_name,
                        type_=self._literalizer.get_sa_type(
                            bi_type=param.incoming_param.user_type,
                            value_base=param.incoming_param.value,
                        ),
                        value=param.incoming_param.value,
                        expanding=isinstance(param.incoming_param.value, (list, tuple)),
                    )
                    for param in formatted_query.bound_params
                ]
            )
        except sa_exc.ArgumentError as e:
            raise WrongQueryParameterization() from e

        return sa_text

    def make_dba_query(self, typed_query: TypedQuery) -> DBAdapterQuery:
        sa_query = self._make_sa_query(typed_query=typed_query)
        dba_query = DBAdapterQuery(query=sa_query, db_name=None)
        return dba_query


@attr.s(frozen=True)
class AsyncTypedQueryAdapterActionViaStandardExecute(AsyncTypedQueryAdapterAction):
    """Executes the typed query via the regular execute adapter method (async)."""

    _query_converter: TypedQueryToDBAQueryConverter = attr.ib(kw_only=True)
    _async_adapter: AsyncDBAdapter = attr.ib(kw_only=True)
    _type_transformer: TypeTransformer = attr.ib(kw_only=True)

    def _resolve_result_column_headers(
        self,
        raw_cursor_info: dict,
        data: list[list],
    ) -> list[TypedQueryResultColumnHeader]:
        # Try to resolve the column count
        column_count: int
        if data:
            column_count = len(data[0])
        else:
            column_count = raw_cursor_info.get("names") or raw_cursor_info.get("db_types") or 0

        # Resolve names
        names: Optional[list[str]] = raw_cursor_info.get("names")
        if not names:
            names = [f"col_{col_num}" for col_num in range(column_count)]

        assert names is not None

        # Resolve native types
        db_types: Optional[list[Optional[GenericNativeType]]] = raw_cursor_info.get("db_types")
        if not db_types:
            db_types = [None for _ in range(column_count)]

        assert db_types is not None

        assert len(names) == column_count
        assert len(db_types) == column_count

        # Convert native types to user types and generate headers
        headers: list[TypedQueryResultColumnHeader] = []
        for name, native_type in zip(names, db_types, strict=True):
            user_type = UserDataType.unsupported
            if native_type:
                try:
                    user_type = self._type_transformer.type_native_to_user(native_type)
                except UnsupportedNativeTypeError:
                    pass

            headers.append(TypedQueryResultColumnHeader(name=name, user_type=user_type))

        return headers

    async def _make_result(
        self, typed_query: TypedQuery, dba_async_result: AsyncRawExecutionResult
    ) -> TypedQueryResult:
        data: list[Any] = []
        async for chunk in dba_async_result.raw_chunk_generator:
            data.extend(chunk)

        column_headers = self._resolve_result_column_headers(
            raw_cursor_info=dba_async_result.raw_cursor_info,
            data=data,
        )
        result = TypedQueryResult(
            query_type=typed_query.query_type,
            data_rows=data,
            column_headers=column_headers,
        )
        return result

    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        assert typed_query.query_type is DashSQLQueryType.generic_query
        dba_query = self._query_converter.make_dba_query(typed_query=typed_query)
        dba_async_result = await self._async_adapter.execute(dba_query)
        result = await self._make_result(typed_query=typed_query, dba_async_result=dba_async_result)
        return result


@attr.s(frozen=True)
class SyncTypedQueryAdapterActionViaLegacyExecute(SyncTypedQueryAdapterAction):
    """Executes the typed query via the regular execute adapter method (sync)."""

    _query_converter: TypedQueryToDBAQueryConverter = attr.ib(kw_only=True)
    _sync_adapter: SyncDirectDBAdapter = attr.ib(kw_only=True)

    def _resolve_result_column_headers(
        self,
        raw_cursor_info: ExecutionStepCursorInfo,
    ) -> list[TypedQueryResultColumnHeader]:
        # TODO: Implement this
        description = raw_cursor_info.raw_cursor_description
        headers: list[TypedQueryResultColumnHeader] = []
        for col_num, _ in enumerate(description):
            headers.append(
                TypedQueryResultColumnHeader(
                    name=f"col_{col_num}",
                    user_type=UserDataType.unsupported,
                )
            )

        return headers

    def _make_result(self, typed_query: TypedQuery, dba_sync_result: DBAdapterQueryResult) -> TypedQueryResult:
        data: list[Any] = dba_sync_result.get_all()
        raw_cursor_info = dba_sync_result.raw_cursor_info
        assert raw_cursor_info is not None
        column_headers = self._resolve_result_column_headers(raw_cursor_info=raw_cursor_info)
        result = TypedQueryResult(
            query_type=typed_query.query_type,
            data_rows=data,
            column_headers=column_headers,
        )
        return result

    def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        assert typed_query.query_type is DashSQLQueryType.generic_query
        dba_query = self._query_converter.make_dba_query(typed_query=typed_query)
        dba_sync_result = self._sync_adapter.execute(dba_query)
        result = self._make_result(typed_query=typed_query, dba_sync_result=dba_sync_result)
        return result
