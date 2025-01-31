from __future__ import annotations

import json
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
        params = dict(
            method=typed_query_raw.parameters.method,
            content_type=typed_query_raw.parameters.content_type,
            body=typed_query_raw.parameters.body,
        )
        dba_query = DBAdapterQuery(
            query=typed_query_raw.parameters.path,
            connector_specific_params=params,
            debug_compiled_query=json.dumps(dict(path=typed_query_raw.parameters.path, **params)),
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
