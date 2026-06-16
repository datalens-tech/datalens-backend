from collections.abc import Callable
import datetime
import logging
import math
from typing import Any

from aiohttp import web

from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.sql_base import SQLBaseView
from dl_api_lib.common_models.data_export import (
    DataExportForbiddenReason,
    DataExportInfo,
)
import dl_api_lib.schemas.main
from dl_api_lib.utils.base import (
    enrich_resp_dict_with_data_export_info,
    get_data_export_base_result,
)
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants import UserDataType
from dl_core.data_processing.dashsql import (
    DashSQLCachedSelector,
    DashSQLEvent,
)
from dl_core.data_processing.sql_selector_base import (
    TResultEvents,
    TRow,
    flatten_rowchunks,
)
from dl_core.us_connection_base import ConnectionBase
from dl_dashsql.exc import DashSQLError
from dl_dashsql.formatting.base import QueryIncomingParameter
from dl_dashsql.types import (
    IncomingDSQLParamType,
    IncomingDSQLParamTypeExt,
)
from dl_query_processing.postprocessing.postprocessors.all import (
    TYPE_PROCESSORS,
    postprocess_array,
)
from dl_query_processing.utils.datetime import parse_datetime

LOGGER = logging.getLogger(__name__)


TRowProcessor = Callable[[TRow], TRow]


def parse_value(value: str | None, bi_type: UserDataType) -> IncomingDSQLParamType:
    if value is None:
        return None
    if bi_type == UserDataType.string:
        return value
    if bi_type == UserDataType.integer:
        return int(value)
    if bi_type == UserDataType.float:
        return float(value)
    if bi_type == UserDataType.date:
        return datetime.date.fromisoformat(value)
    if bi_type == UserDataType.datetime:
        return parse_datetime(value)
    if bi_type == UserDataType.boolean:
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"Not a valid boolean: {value!r}")
    raise DashSQLError(f"Unsupported type: {bi_type.name!r}")


def make_param_obj(name: str, param: dict) -> QueryIncomingParameter:
    type_name: str = param["type_name"]
    value_base: str | list[str] | tuple[str, ...] = param["value"]
    value: IncomingDSQLParamTypeExt

    try:
        bi_type = UserDataType[type_name]
    except KeyError:
        raise DashSQLError(f"Unknown type name {type_name!r}") from None

    try:
        if isinstance(value_base, (list, tuple)):
            value = tuple(parse_value(sub_value, bi_type) for sub_value in value_base)
        else:
            value = parse_value(value_base, bi_type)
    except ValueError as e:
        raise DashSQLError(f"Unsupported value for type {type_name!r}: {value_base!r}") from e

    return QueryIncomingParameter(
        original_name=name,
        user_type=bi_type,
        value=value,
    )


class DashSQLView(SQLBaseView):
    """
    Connection + SQL query -> special-format data result.

    Partially related to `DatasetPreviewView` and its bases, but much more stripped down.
    """

    endpoint_code = "DashSQL"
    profiler_prefix = "dashsql_result"
    dashsql_selector_cls: type[DashSQLCachedSelector] = DashSQLCachedSelector

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

    @classmethod
    def _postprocess_key(cls, value: Any) -> str:
        return str(value)

    @classmethod
    def _postprocess_any(cls, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return tuple(cls._postprocess_any(sub_value) for sub_value in value)
        if isinstance(value, dict):
            return {
                cls._postprocess_key(sub_key): cls._postprocess_any(sub_value) for sub_key, sub_value in value.items()
            }
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return str(value)
        return value

    @classmethod
    def _get_type_processor(cls, bi_type_name: str) -> Callable[[Any], Any]:
        bi_type = UserDataType.__members__.get(bi_type_name)

        if bi_type is None:
            return lambda val: val

        # Handle datetimetz as datetime (without timezone conversion)
        if bi_type == UserDataType.datetimetz:
            bi_type = UserDataType.datetime

        type_processor = TYPE_PROCESSORS.get(bi_type)

        # Return arrays as is (no need to dump), otherwise use the processor
        if type_processor is None or type_processor == postprocess_array:
            return lambda val: val

        return type_processor

    @classmethod
    def _make_postprocess_row(cls, bi_type_names: tuple[str, ...]) -> TRowProcessor:
        type_processors = tuple(cls._get_type_processor(name) for name in bi_type_names)

        def _postprocess_row(row: TRow) -> TRow:
            result = []
            for proc, val in zip(type_processors, row, strict=True):
                try:
                    result.append(cls._postprocess_any(proc(val)))
                except (ValueError, TypeError):
                    LOGGER.warning("Failed to postprocess value %s (processor: %s). Using raw value.", val, proc)
                    result.append(cls._postprocess_any(val))
            return tuple(result)

        return _postprocess_row

    async def _postprocess_events(self, result_events: TResultEvents) -> TResultEvents:
        postprocess_row: TRowProcessor | None = None
        async for event_name, event_data in result_events:
            if postprocess_row is None and event_name == DashSQLEvent.metadata.value:
                assert isinstance(event_data, dict)
                postprocess_row = self._make_postprocess_row(event_data["bi_types"])  # type: ignore  # TODO: fix
            elif event_name == DashSQLEvent.rowchunk.value and postprocess_row is not None:
                event_data = tuple(postprocess_row(row) for row in event_data)  # type: ignore  # TODO: fix
            elif event_name == DashSQLEvent.row.value and postprocess_row is not None:  # should be obsolete
                event_data = postprocess_row(event_data)  # type: ignore  # TODO: fix
            yield event_name, event_data

    async def collect_result_events_into_response(
        self, result_events: TResultEvents, conn: ConnectionBase
    ) -> web.Response:
        events: list = []
        data_export_info = self.get_data_export_info(conn)
        data_export_result = get_data_export_base_result(data_export_info)
        data_export_result.background.allowed = False
        data_export_result.background.reason.append(DataExportForbiddenReason.prohibited_in_dashsql.value)

        async for event_name, event_data in result_events:
            if event_name == DashSQLEvent.metadata.value:
                assert isinstance(event_data, dict)
                enrich_resp_dict_with_data_export_info(event_data, data_export_result)

            events.append({"event": event_name, "data": event_data})

        return self._json_sql_response(events)

    async def collect_dashsql_response(self, result_events: TResultEvents, conn: ConnectionBase) -> web.Response:
        events: list = []
        async for event_name, event_data in result_events:
            events.append({"event": event_name, "data": event_data})

        resp_data = {"events": events}

        data_export_info = self.get_data_export_info(conn)
        data_export_result = get_data_export_base_result(data_export_info)
        data_export_result.background.allowed = False
        data_export_result.background.reason.append(DataExportForbiddenReason.prohibited_in_dashsql.value)
        enrich_resp_dict_with_data_export_info(resp_data, data_export_result)

        return self._json_sql_response(resp_data)

    @generic_profiler_async("dashsql-result")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        _conn_id, conn = await self._resolve_raw_sql_connection()

        schema = dl_api_lib.schemas.main.DashSQLRequestSchema()
        body = schema.load(self.dl_request.json)
        sql_query = body["sql_query"]
        raw_params = body.get("params")
        db_params = body.get("db_params")
        connector_specific_params = body.get("connector_specific_params")

        incoming_parameters: list[QueryIncomingParameter] | None = None
        if raw_params is not None:
            incoming_parameters = [make_param_obj(name, param) for name, param in raw_params.items()]
            keeper = self.dl_request.rci.secret_keeper
            for name, param in raw_params.items():
                self.register_param_values(keeper, name, param["value"])

        # TODO: move dashsql selector's construction to factory
        bleeding_edge_users = self.request.app.get("BLEEDING_EDGE_USERS", ())
        is_bleeding_edge_user = self.dl_request.rci.user_name in bleeding_edge_users

        dashsql_selector = self.dashsql_selector_cls(
            conn=conn,
            sql_query=sql_query,
            # Note: `None` means `no substitution` (legacy),
            # `[]` means `attempt to substitute`.
            incoming_parameters=incoming_parameters,
            db_params=db_params or {},
            connector_specific_params=connector_specific_params,
            service_registry=self.dl_request.services_registry,
            is_bleeding_edge_user=is_bleeding_edge_user,
        )
        result_events = await dashsql_selector.execute()
        result_events = self._postprocess_events(result_events)

        # TODO: modify API consumers to allow `rowchunk` events;
        # but for now, unroll them into `row` events.
        result_events = flatten_rowchunks(result_events)

        with_export_info = self.request.query.get("with_export_info", False)
        if with_export_info:
            resp = await self.collect_dashsql_response(result_events=result_events, conn=conn)  # TODO: remove
        else:
            resp = await self.collect_result_events_into_response(result_events=result_events, conn=conn)

        return resp
