from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
)

import attr
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import DashSQLQueryType
from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncTypedQueryAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.sync_base import SyncTypedQueryAdapterAction
from dl_core.connection_executors.adapters.adapters_base import (
    DBAdapterQueryResult,
    SyncDirectDBAdapter,
)
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
import dl_core.exc as exc
from dl_dashsql.formatting.base import (
    QueryFormatterFactory,
    QueryIncomingParameter,
)
from dl_dashsql.literalizer import DashSQLParamLiteralizer
from dl_dashsql.typed_query.primitives import (
    DataRowsTypedQueryResult,
    PlainTypedQuery,
    TypedQuery,
    TypedQueryResult,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.adapters.async_adapters_base import AsyncDBAdapter


@attr.s(frozen=True)
class AsyncTypedQueryAdapterActionEmptyDataRows(AsyncTypedQueryAdapterAction):
    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        return DataRowsTypedQueryResult(
            query_type=typed_query.query_type,
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
                user_type=param.user_type,
                value=param.value,
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
        except sa_exc.ArgumentError:
            raise exc.WrongQueryParameterization()

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

    async def _make_result(
        self, typed_query: TypedQuery, dba_async_result: AsyncRawExecutionResult
    ) -> TypedQueryResult:
        data: list[Any] = []
        async for chunk in dba_async_result.raw_chunk_generator:
            data.extend(chunk)

        result = DataRowsTypedQueryResult(
            query_type=typed_query.query_type,
            data_rows=data,
        )
        return result

    async def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        assert typed_query.query_type is DashSQLQueryType.classic_query
        dba_query = self._query_converter.make_dba_query(typed_query=typed_query)
        dba_async_result = await self._async_adapter.execute(dba_query)
        result = await self._make_result(typed_query=typed_query, dba_async_result=dba_async_result)
        return result


@attr.s(frozen=True)
class SyncTypedQueryAdapterActionViaLegacyExecute(SyncTypedQueryAdapterAction):
    """Executes the typed query via the regular execute adapter method (sync)."""

    _query_converter: TypedQueryToDBAQueryConverter = attr.ib(kw_only=True)
    _sync_adapter: SyncDirectDBAdapter = attr.ib(kw_only=True)

    def _make_result(self, typed_query: TypedQuery, dba_sync_result: DBAdapterQueryResult) -> TypedQueryResult:
        data: list[Any] = dba_sync_result.get_all()
        result = DataRowsTypedQueryResult(
            query_type=typed_query.query_type,
            data_rows=data,
        )
        return result

    def run_typed_query_action(self, typed_query: TypedQuery) -> TypedQueryResult:
        assert typed_query.query_type is DashSQLQueryType.classic_query
        dba_query = self._query_converter.make_dba_query(typed_query=typed_query)
        dba_sync_result = self._sync_adapter.execute(dba_query)
        result = self._make_result(typed_query=typed_query, dba_sync_result=dba_sync_result)
        return result