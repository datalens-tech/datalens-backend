from __future__ import annotations

import abc
from typing import (
    Any,
    Optional,
)

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.typed_query import (
    PlainTypedQueryContentSchema,
    RawTypedQuery,
    RawTypedQueryParameter,
    TypedQuerySchema,
)
from dl_api_lib.utils.base import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import DashSQLQueryType
from dl_core.us_connection_base import ConnectionBase
from dl_dashsql.typed_query.primitives import (
    DataRowsTypedQueryResult,
    PlainTypedQuery,
    TypedQuery,
    TypedQueryParameter,
    TypedQueryResult,
)


class TypedQueryResultSerializer:
    def serialize_typed_query_result(self, typed_query_result: TypedQueryResult) -> Any:
        assert isinstance(typed_query_result, DataRowsTypedQueryResult)
        return list(typed_query_result.data_rows)


class TypedQueryLoader(abc.ABC):
    @abc.abstractmethod
    def load_typed_query(
        self,
        query_type: DashSQLQueryType,
        query_content: dict,
        parameters: list[RawTypedQueryParameter],
    ) -> TypedQuery:
        raise NotImplementedError


class PlainTypedQueryLoader(TypedQueryLoader):
    def load_typed_query(
        self,
        query_type: DashSQLQueryType,
        query_content: dict,
        parameters: list[RawTypedQueryParameter],
    ) -> TypedQuery:
        query_content = PlainTypedQueryContentSchema().load(query_content)
        typed_query = PlainTypedQuery(
            query_type=query_type,
            query=query_content["query"],
            parameters=tuple(
                TypedQueryParameter(
                    name=param.name,
                    user_type=param.data_type,
                    value=param.value.value,
                )
                for param in parameters
            ),
        )
        return typed_query


@requires(RequiredResourceCommon.US_MANAGER)
class DashSQLTypedQueryView(BaseView):
    """
    Connection + SQL query -> special-format data result.

    Partially related to `DatasetPreviewView` and its bases, but much more stripped down.
    """

    # TODO?: cache support

    endpoint_code = "DashSQLTypedQuery"
    profiler_prefix = "dashsql_typed_query"

    @property
    def connection_id(self) -> Optional[str]:
        return self.request.match_info.get("conn_id")

    async def get_connection(self) -> ConnectionBase:
        connection_id = self.connection_id
        assert connection_id
        connection = await self.dl_request.us_manager.get_by_id(connection_id, ConnectionBase)
        assert isinstance(connection, ConnectionBase)
        return connection

    def validate_connection(self, connection: ConnectionBase) -> None:
        need_permission_on_entry(connection, USPermissionKind.execute)

    def make_typed_query(self) -> TypedQuery:
        raw_typed_query: RawTypedQuery = TypedQuerySchema().load(self.dl_request.json)
        loader = PlainTypedQueryLoader()  # TODO: Get loader from somewhere using query_type
        typed_query = loader.load_typed_query(
            query_type=raw_typed_query.query_type,
            query_content=raw_typed_query.query_content,
            parameters=raw_typed_query.parameters,
        )
        return typed_query

    async def execute_query(self, connection: ConnectionBase, typed_query: TypedQuery) -> TypedQueryResult:
        sr = self.dl_request.services_registry
        ce_factory = sr.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(conn=connection)
        typed_query_result = await conn_executor.execute_typed_query(typed_query=typed_query)
        return typed_query_result

    def make_response_data(self, typed_query_result: TypedQueryResult) -> dict:
        result_serializer = TypedQueryResultSerializer()  # TODO: Get serializer from somewhere
        response_data = {"data": result_serializer.serialize_typed_query_result(typed_query_result)}
        return response_data

    @generic_profiler_async("dashsql-typed-query")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        connection = await self.get_connection()
        self.validate_connection(connection)

        typed_query = self.make_typed_query()
        typed_query_result = await self.execute_query(connection=connection, typed_query=typed_query)

        return web.json_response(self.make_response_data(typed_query_result))
