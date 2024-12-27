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
from dl_api_lib.schemas.typed_query_raw import (
    RawTypedQueryRaw,
    TypedQueryRawSchema,
    TypedQueryRawResultSchema
)
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_api_lib.utils.base import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import DashSQLQueryType
import dl_core.exc as core_exc
from dl_core.us_connection_base import ConnectionBase
from dl_dashsql.typed_query.primitives import (
    TypedQueryRaw,
    TypedQueryRawParameters,
    TypedQueryRawResult,
)


@requires(RequiredResourceCommon.US_MANAGER)
class DashSQLTypedQueryRawView(BaseView):
    """
    Perform a query using the given connection.
    The behavior is connection- and query type-specific.

    Main use cases:
    - QL charts (charts using user-written queries)
    - selectors for QL charts
    """

    endpoint_code = "DashSQLTypedQueryRaw"
    profiler_prefix = "dashsql_typed_query_raw"

    @property
    def api_service_registry(self) -> ApiServiceRegistry:
        service_registry = self.dl_request.services_registry
        assert isinstance(service_registry, ApiServiceRegistry)
        return service_registry

    @property
    def connection_id(self) -> Optional[str]:
        return self.request.match_info.get("conn_id")

    async def get_connection(self) -> ConnectionBase:
        """Get connection object from the ID in the URL"""
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

    def make_typed_query(self) -> TypedQueryRaw:
        """Formalize and validate query from input"""
        raw_typed_query: RawTypedQueryRaw = TypedQueryRawSchema().load(self.dl_request.json)
        typed_query = TypedQueryRaw(
            query_type=raw_typed_query.query_type,
            parameters=TypedQueryRawParameters(
                path=raw_typed_query.parameters.path,
                method=raw_typed_query.parameters.method,
                body=raw_typed_query.parameters.body
            )
        )
        return typed_query

    async def execute_query(self, connection: ConnectionBase, typed_query: TypedQueryRaw) -> TypedQueryRawResult:
        """Prepare everything for execution and execute"""
        tq_processor_factory = self.api_service_registry.get_typed_query_raw_processor_factory()
        tq_processor = tq_processor_factory.get_typed_query_processor(connection=connection)
        typed_query_result = await tq_processor.process_typed_query_raw(typed_query_raw=typed_query)
        return typed_query_result

    def make_response_data(self, typed_query_result: TypedQueryRawResult) -> dict:
        """Serialize output"""
        return {
            "query_type": typed_query_result.query_type.name,
            "data": TypedQueryRawResultSchema().dump(typed_query_result),
        }

    @generic_profiler_async("dashsql-typed-query-raw")
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
