from __future__ import annotations

import datetime
import decimal
import json
import math
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Type,
)
import uuid

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.enums import USPermissionKind
import dl_api_lib.schemas.main
from dl_api_lib.utils.base import need_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.backend_types import get_backend_type
from dl_core.dashsql.literalizer import TValueBase
from dl_core.dashsql.registry import get_dash_sql_param_literalizer
from dl_core.data_processing.dashsql import (
    BindParamInfo,
    DashSQLCachedSelector,
    DashSQLEvent,
    TRow,
)
from dl_core.exc import (
    DashSQLError,
    UnexpectedUSEntryType,
)
from dl_core.us_connection_base import (
    ConnectionBase,
    ExecutorBasedMixin,
    SubselectMixin,
)
from dl_query_processing.utils.datetime import parse_datetime


if TYPE_CHECKING:
    from dl_constants.types import TJSONLike
    from dl_core.data_processing.dashsql import (
        DashSQLSelector,
        TResultEvents,
    )


TRowProcessor = Callable[[TRow], TRow]


def parse_value(value: Optional[str], bi_type: UserDataType) -> Any:
    if value is None:
        return None
    if bi_type == UserDataType.string:
        return value
    if bi_type == UserDataType.integer:
        return int(value)
    if bi_type == UserDataType.float:
        return float(value)
    if bi_type == UserDataType.date:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    if bi_type == UserDataType.datetime:
        return parse_datetime(value)
    if bi_type == UserDataType.boolean:
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"Not a valid boolean: {value!r}")
    raise DashSQLError(f"Unsupported type: {bi_type.name!r}")


def make_param_obj(name: str, param: dict, conn_type: ConnectionType) -> BindParamInfo:
    type_name: str = param["type_name"]
    value_base: TValueBase = param["value"]

    try:
        bi_type = UserDataType[type_name]
    except KeyError:
        raise DashSQLError(f"Unknown type name {type_name!r}")

    backend_type = get_backend_type(conn_type=conn_type)
    literalizer_cls = get_dash_sql_param_literalizer(backend_type=backend_type)
    sa_type = literalizer_cls.get_sa_type(bi_type=bi_type, value_base=value_base)

    try:
        if isinstance(value_base, (list, tuple)):
            value = [parse_value(sub_value, bi_type) for sub_value in value_base]
        else:
            value = parse_value(value_base, bi_type)
    except ValueError:
        raise DashSQLError(f"Unsupported value for type {type_name!r}: {value_base!r}")

    return BindParamInfo(
        name=name,
        type_name=type_name,
        sa_type=sa_type,
        value=value,
        expanding=isinstance(value, (list, tuple)),
    )


@requires(RequiredResourceCommon.US_MANAGER)
class DashSQLView(BaseView):
    """
    Connection + SQL query -> special-format data result.

    Partially related to `DatasetPreviewView` and its bases, but much more stripped down.
    """

    # TODO?: cache support

    endpoint_code = "DashSQL"
    profiler_prefix = "dashsql_result"
    dashsql_selector_cls: Type[DashSQLSelector] = DashSQLCachedSelector

    @property
    def conn_id(self) -> Optional[str]:
        return self.request.match_info.get("conn_id")

    def enrich_logging_context(self, conn: ConnectionBase) -> None:
        """
        See also:
        `dl_api_lib.app.data_api.resources.dataset.base.DatasetDataBaseView.default_query_execution_cm_stack`
        """
        if not self.dl_request.log_ctx_controller:
            # TODO?: warn
            return
        self.dl_request.log_ctx_controller.put_to_context("conn_type", conn.conn_type.name)
        sr = self.dl_request.services_registry
        if isinstance(conn, ExecutorBasedMixin):
            ce_cls_str = sr.get_conn_executor_factory().get_async_conn_executor_cls(conn).__qualname__
            self.dl_request.log_ctx_controller.put_to_context("conn_exec_cls", ce_cls_str)

    @staticmethod
    def _json_default(value: Any) -> TJSONLike:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, datetime.timedelta):
            return value.total_seconds()
        if isinstance(value, decimal.Decimal):
            return str(value)
        if isinstance(value, uuid.UUID):
            return str(value)
        raise DashSQLError("Unexpected value type in dashsql serialization: {!r}".format(type(value)))

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
    def _make_postprocess_row(cls, bi_type_names: tuple[str, ...]) -> TRowProcessor:
        # Nothing type-specific at the moment
        funcs = tuple(cls._postprocess_any for _ in bi_type_names)

        def _postprocess_row(row: TRow, funcs: tuple[Callable, ...] = funcs) -> TRow:
            return tuple(func(value) for func, value in zip(funcs, row))

        return _postprocess_row

    async def _postprocess_events(self, result_events: TResultEvents) -> TResultEvents:
        postprocess_row: Optional[TRowProcessor] = None
        async for event_name, event_data in result_events:
            if postprocess_row is None and event_name == DashSQLEvent.metadata.value:
                assert isinstance(event_data, dict)
                postprocess_row = self._make_postprocess_row(event_data["bi_types"])  # type: ignore  # TODO: fix
            elif event_name == DashSQLEvent.rowchunk.value and postprocess_row is not None:
                event_data = tuple(postprocess_row(row) for row in event_data)  # type: ignore  # TODO: fix
            elif event_name == DashSQLEvent.row.value and postprocess_row is not None:  # should be obsolete
                event_data = postprocess_row(event_data)  # type: ignore  # TODO: fix
            yield event_name, event_data

    @staticmethod
    async def _flatten_chunk_events(result_events: TResultEvents) -> TResultEvents:
        """Unwrap `rowchunk` events into `row` events, to avoid changing the protocol (for now)"""
        async for event_name, event_data in result_events:
            if event_name == DashSQLEvent.rowchunk.value:
                for row in event_data:
                    assert isinstance(row, (tuple, list))
                    yield DashSQLEvent.row.value, row  # type: ignore  # TODO: fix
            else:
                yield event_name, event_data

    async def collect_result_events_into_response(self, result_events: TResultEvents) -> web.Response:
        events: list = []
        async for event_name, event_data in result_events:
            events.append(dict(event=event_name, data=event_data))

        data = json.dumps(events, default=self._json_default)
        response = web.Response(
            body=data.encode(),
            content_type="application/json",
        )
        return response

    @generic_profiler_async("dashsql-result")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        conn_id = self.conn_id
        assert conn_id

        if self.dl_request.log_ctx_controller:
            self.dl_request.log_ctx_controller.put_to_context("conn_id", conn_id)

        schema = dl_api_lib.schemas.main.DashSQLRequestSchema()
        body = schema.load(self.dl_request.json)
        sql_query = body["sql_query"]
        params = body.get("params")
        db_params = body.get("db_params")
        connector_specific_params = body.get("connector_specific_params")

        conn = await self.dl_request.us_manager.get_by_id(conn_id, ConnectionBase)
        assert isinstance(conn, ConnectionBase)

        self.enrich_logging_context(conn)

        if not isinstance(conn, SubselectMixin) or not isinstance(conn, ExecutorBasedMixin):
            raise UnexpectedUSEntryType("Expecting a subselect-compatible connection")

        # A slightly more explicit check (which should have been done in `get_by_id` anyway):
        need_permission_on_entry(conn, USPermissionKind.execute)

        # TODO: instead of this, use something like:
        #     formula_dialect = dl_formula.core.dialect.from_name_and_version(conn.get_dialect().name)
        #     bindparam = dl_formula.definitions.literals.literal(parsed_value, formula_dialect)
        # (but account for `expanding`)
        conn_type = conn.conn_type
        param_objs = None
        if params is not None:
            param_objs = [make_param_obj(name, param, conn_type=conn_type) for name, param in params.items()]

        # TODO: move dashsql selector's construction to factory
        bleeding_edge_users = self.request.app.get("BLEEDING_EDGE_USERS", ())
        is_bleeding_edge_user = self.dl_request.rci.user_name in bleeding_edge_users

        dashsql_selector = self.dashsql_selector_cls(
            conn=conn,
            sql_query=sql_query,
            # Note: `None` means `no substitution` (legacy),
            # `[]` means `attempt to substitute`.
            params=param_objs,
            db_params=db_params or {},
            connector_specific_params=connector_specific_params,
            service_registry=self.dl_request.services_registry,
            is_bleeding_edge_user=is_bleeding_edge_user,
        )
        result_events = await dashsql_selector.execute()
        result_events = self._postprocess_events(result_events)

        # TODO: modify API consumers to allow `rowchunk` events;
        # but for now, unroll them into `row` events.
        result_events = self._flatten_chunk_events(result_events)

        return await self.collect_result_events_into_response(result_events)
