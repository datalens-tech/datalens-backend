from __future__ import annotations

import json
import sys
import time
from typing import Optional

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_commons.reporting.models import (
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.typed_query_raw import (
    RawTypedQueryRaw,
    TypedQueryRawResultSchema,
    TypedQueryRawSchema,
)
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_api_lib.utils.base import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_core.base_models import WorkbookEntryLocation
import dl_core.exc as core_exc
from dl_core.us_connection_base import ConnectionBase
from dl_core.utils import make_id
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
        if not connection.is_typed_query_raw_allowed:
            raise core_exc.DashSQLNotAllowed()

    def make_typed_query_raw(self) -> TypedQueryRaw:
        """Formalize and validate query from input"""
        raw_typed_query_raw: RawTypedQueryRaw = TypedQueryRawSchema().load(self.dl_request.json)
        typed_query_raw = TypedQueryRaw(
            query_type=raw_typed_query_raw.query_type,
            parameters=TypedQueryRawParameters(
                path=raw_typed_query_raw.parameters.path,
                method=raw_typed_query_raw.parameters.method,
                content_type=raw_typed_query_raw.parameters.content_type,
                body=raw_typed_query_raw.parameters.body,
            ),
        )
        return typed_query_raw

    async def execute_query(self, connection: ConnectionBase, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        """Prepare everything for execution and execute"""

        def _make_reporting_query() -> str:
            params = typed_query_raw.parameters
            return json.dumps(
                dict(
                    path=params.path,
                    method=params.method,
                    content_type=params.content_type,
                    body="***" if params.body else None,
                ),
                sort_keys=True,
            )

        tq_processor_factory = self.api_service_registry.get_typed_query_raw_processor_factory()
        tq_processor = tq_processor_factory.get_typed_query_processor(connection=connection)

        # add reporting
        reporting_registry = self.api_service_registry.get_reporting_registry()
        workbook_id = self.api_service_registry.rci.workbook_id or (
            connection.entry_key.workbook_id if isinstance(connection.entry_key, WorkbookEntryLocation) else None
        )
        query_id = f"typed_query_raw_{make_id()}"

        reporting_registry.save_reporting_record(
            QueryExecutionStartReportingRecord(
                timestamp=time.time(),
                query_id=query_id,
                dataset_id=None,
                query_type=None,
                connection_type=connection.conn_type,
                conn_reporting_data=connection.get_conn_dto().conn_reporting_data(),
                query=_make_reporting_query(),
                workbook_id=workbook_id,
            )
        )
        try:
            # execute
            return await tq_processor.process_typed_query_raw(typed_query_raw=typed_query_raw)
        finally:
            # save end reporting
            _, exec_exception, _ = sys.exc_info()
            reporting_registry.save_reporting_record(
                QueryExecutionEndReportingRecord(
                    timestamp=time.time(),
                    query_id=query_id,
                    exception=exec_exception,
                )
            )

    def make_response_data(self, typed_query_raw_result: TypedQueryRawResult) -> dict:
        """Serialize output"""
        return TypedQueryRawResultSchema().dump(typed_query_raw_result)

    @generic_profiler_async("dashsql-typed-query-raw")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        """The main view method. Handle typed query execution"""

        # Formalize and validate input
        connection = await self.get_connection()
        self.validate_connection(connection)
        typed_query_raw = self.make_typed_query_raw()

        # Execute
        typed_query_raw_result = await self.execute_query(connection=connection, typed_query_raw=typed_query_raw)

        # Prepare and return output
        return web.json_response(self.make_response_data(typed_query_raw_result))
