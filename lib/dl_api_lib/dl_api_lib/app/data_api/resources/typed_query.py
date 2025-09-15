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
from dl_api_lib.common_models.data_export import (
    DataExportForbiddenReason,
    DataExportInfo,
    DataExportResult,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.typed_query import (
    PlainTypedQueryContentSchema,
    RawTypedQuery,
    RawTypedQueryParameter,
    TypedQueryResultSchema,
    TypedQuerySchema,
)
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_api_lib.utils.base import (
    enrich_resp_dict_with_data_export_info,
    get_data_export_base_result,
    need_permission_on_entry,
)
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import DashSQLQueryType
import dl_core.exc as core_exc
from dl_core.us_connection_base import ConnectionBase
from dl_dashsql.typed_query.primitives import (
    PlainTypedQuery,
    TypedQuery,
    TypedQueryParameter,
    TypedQueryResult,
)


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
                    typed_value=param.value,
                )
                for param in parameters
            ),
        )
        return typed_query


class TypedQueryResultSerializer:
    """Serializes the result (meta and data)"""

    def serialize_typed_query_result(self, typed_query_result: TypedQueryResult) -> Any:
        return {
            "query_type": typed_query_result.query_type.name,
            "data": TypedQueryResultSchema().dump(typed_query_result),
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
    def api_service_registry(self) -> ApiServiceRegistry:
        service_registry = self.dl_request.services_registry
        assert isinstance(service_registry, ApiServiceRegistry)
        return service_registry

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

    async def execute_query(self, connection: ConnectionBase, typed_query: TypedQuery) -> TypedQueryResult:
        """Prepare everything for execution and execute"""
        tq_processor_factory = self.api_service_registry.get_typed_query_processor_factory()
        tq_processor = tq_processor_factory.get_typed_query_processor(connection=connection)
        typed_query_result = await tq_processor.process_typed_query(typed_query=typed_query)
        return typed_query_result

    def make_response_data(self, typed_query_result: TypedQueryResult, data_export_result: DataExportResult) -> dict:
        """Serialize output"""
        result_serializer = TypedQueryResultSerializer()  # TODO: Get serializer from somewhere
        response_data = result_serializer.serialize_typed_query_result(typed_query_result)
        enrich_resp_dict_with_data_export_info(response_data, data_export_result)
        return response_data

    def get_data_export_info(self, conn: ConnectionBase) -> DataExportInfo:
        tenant = self.dl_request.rci.tenant
        assert tenant
        return DataExportInfo(
            enabled_in_conn=not conn.data.data_export_forbidden,
            enabled_in_ds=True,
            enabled_in_tenant=tenant.is_data_export_enabled,
            allowed_in_conn_type=conn.allow_background_data_export_for_conn_type,
            background_allowed_in_tenant=tenant.is_background_data_export_allowed,
        )

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

        data_export_info = self.get_data_export_info(conn=connection)
        data_export_result = get_data_export_base_result(data_export_info)
        data_export_result.background.allowed = False
        data_export_result.background.reason.append(DataExportForbiddenReason.prohibited_in_typed_query.value)

        # Prepare and return output
        return web.json_response(self.make_response_data(typed_query_result, data_export_result))
