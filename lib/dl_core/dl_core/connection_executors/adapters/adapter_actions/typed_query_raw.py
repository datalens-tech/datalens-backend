from __future__ import annotations

from typing import TYPE_CHECKING

import attr

from dl_constants.enums import DashSQLQueryType
from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncTypedQueryRawAdapterAction
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawJsonExecutionResult
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_dashsql.typed_query.primitives import (
    TypedQueryRaw,
    TypedQueryRawResult,
    TypedQueryRawResultData,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.adapters.async_adapters_base import AsyncDBAdapter


@attr.s(frozen=True)
class TypedQueryRawToDBAQueryConverter:
    """
    Converts TypedQueryRaw instance to DBAdapterQuery instance
    for compatibility with the execute adapter method.
    """

    def make_dba_query(self, typed_query_raw: TypedQueryRaw) -> DBAdapterQuery:
        params = typed_query_raw.parameters
        dba_query = DBAdapterQuery(
            query=params.path,
            connector_specific_params=dict(
                method=params.method,
                content_type=params.content_type,
                body=params.body,
            ),
            debug_compiled_query=f"{params.method} {params.path} [Content-Type: {params.content_type}]",
        )
        return dba_query


@attr.s(frozen=True)
class AsyncTypedQueryRawAdapterActionViaStandardExecute(AsyncTypedQueryRawAdapterAction):
    """Executes the typed query via the regular execute adapter method (async)."""

    _query_converter: TypedQueryRawToDBAQueryConverter = attr.ib(kw_only=True)
    _async_adapter: AsyncDBAdapter = attr.ib(kw_only=True)

    async def run_typed_query_raw_action(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        assert typed_query_raw.query_type is DashSQLQueryType.raw_query
        dba_query = self._query_converter.make_dba_query(typed_query_raw=typed_query_raw)
        dba_async_result: AsyncRawJsonExecutionResult = await self._async_adapter.execute(dba_query)  # type: ignore

        result_data = TypedQueryRawResultData(
            status=dba_async_result.raw_data["status"],
            headers=dba_async_result.raw_data["headers"],
            body=dba_async_result.raw_data["body"],
        )
        result = TypedQueryRawResult(query_type=typed_query_raw.query_type, data=result_data)
        return result
