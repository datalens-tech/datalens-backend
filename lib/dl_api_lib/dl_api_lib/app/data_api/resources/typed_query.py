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
    DataRowsTypedQueryResultSchema,
    PlainTypedQueryContentSchema,
    RawTypedQuery,
    RawTypedQueryParameter,
    TypedQuerySchema,
)
from dl_api_lib.utils.base import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import DashSQLQueryType
from dl_core.data_processing.typed_query import CEBasedTypedQueryProcessor
import dl_core.exc as core_exc
from dl_core.us_connection_base import ConnectionBase
from dl_dashsql.typed_query.primitives import (
    DataRowsTypedQueryResult,
    PlainTypedQuery,
    TypedQuery,
    TypedQueryParameter,
    TypedQueryResult,
)
from dl_dashsql.typed_query.processor.base import TypedQueryProcessorBase


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


class TypedQueryResultSerializer:
    """Serializes the result (meta and data)"""

    def serialize_typed_query_result(self, typed_query_result: TypedQueryResult) -> Any:
        # No other result types are supported in API:
        assert isinstance(typed_query_result, DataRowsTypedQueryResult)
        return {
            "query_type": typed_query_result.query_type.name,
            "data": DataRowsTypedQueryResultSchema().dump(typed_query_result),
        }


@requires(RequiredResourceCommon.US_MANAGER)
class DashSQLTypedQueryView(BaseView):
    """
    Perform a query using the given connection.
    The behavior is connection- and query type-specific.

    Main use cases:
    - QL charts (charts using user-written queries)
    - selectors for QL charts
    """

    # TODO?: cache support

    endpoint_code = "DashSQLTypedQuery"
    profiler_prefix = "dashsql_typed_query"

    @property
    def connection_id(self) -> Optional[str]:
        # TODO: Move to some base class for connection-based views
        return self.request.match_info.get("conn_id")

    async def get_connection(self) -> ConnectionBase:
        """Get connection object from the ID in the URL"""
        # TODO: Move to some base class for connection-based views
        connection_id = self.connection_id
        assert connection_id
        connection = await self.dl_request.us_manager.get_by_id(connection_id, ConnectionBase)
        assert isinstance(connection, ConnectionBase)
        return connection

    def validate_connection(self, connection: ConnectionBase) -> None:
        """Check whether we can use this connection to execute the query"""
        need_permission_on_entry(connection, USPermissionKind.execute)
        if not connection.is_typed_query_allowed or not connection.is_dashsql_allowed:
            raise core_exc.DashSQLNotAllowed()

    def make_typed_query(self) -> TypedQuery:
        """Formalize and validate query from input"""
        raw_typed_query: RawTypedQuery = TypedQuerySchema().load(self.dl_request.json)
        loader = PlainTypedQueryLoader()  # TODO: Get loader from somewhere using query_type
        typed_query = loader.load_typed_query(
            query_type=raw_typed_query.query_type,
            query_content=raw_typed_query.query_content,
            parameters=raw_typed_query.parameters,
        )
        return typed_query

    def _get_tq_processor(self, connection: ConnectionBase) -> TypedQueryProcessorBase:
        # TODO: Move processor creation to SR (more logic will come with the implementation of caches)
        sr = self.dl_request.services_registry
        ce_factory = sr.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(conn=connection)
        tq_processor = CEBasedTypedQueryProcessor(async_conn_executor=conn_executor)
        return tq_processor

    async def execute_query(self, connection: ConnectionBase, typed_query: TypedQuery) -> TypedQueryResult:
        """Prepare everything for execution and execute"""
        tq_processor = self._get_tq_processor(connection=connection)
        typed_query_result = await tq_processor.process_typed_query(typed_query=typed_query)
        return typed_query_result

    def make_response_data(self, typed_query_result: TypedQueryResult) -> dict:
        """Serialize output"""
        result_serializer = TypedQueryResultSerializer()  # TODO: Get serializer from somewhere
        response_data = result_serializer.serialize_typed_query_result(typed_query_result)
        return response_data

    @generic_profiler_async("dashsql-typed-query")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        """The main view method. Handle typed query execution"""

        # Formalize and validate input
        connection = await self.get_connection()
        self.validate_connection(connection)
        typed_query = self.make_typed_query()

        # Execute
        typed_query_result = await self.execute_query(connection=connection, typed_query=typed_query)

        # Prepare and return output
        return web.json_response(self.make_response_data(typed_query_result))
