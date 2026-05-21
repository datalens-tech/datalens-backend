import datetime
import decimal
import json
import math
from typing import Any
import uuid

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    requires,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.utils.base import need_permission_on_entry
from dl_constants.types import TJSONLike
from dl_core.exc import UnexpectedUSEntryType
from dl_core.us_connection_base import (
    ConnectionBase,
    RawSqlLevelConnectionMixin,
)


@requires(RequiredResourceCommon.US_MANAGER)
class SQLBaseView(BaseView):
    """Common scaffolding for raw-SQL endpoints.

    Subclasses are expected to:
    - call `_resolve_raw_sql_connection()` at the top of their handler;
    - use `_json_sql_response(payload)` to serialize the wire response.

    Override `_enrich_logging_context_with_conn` to push extra fields into
    the log context for a particular endpoint.
    """

    @property
    def conn_id(self) -> str | None:
        return self.request.match_info.get("conn_id")

    async def _resolve_raw_sql_connection(self) -> tuple[str, ConnectionBase]:
        conn_id = self.conn_id
        assert conn_id

        if self.dl_request.log_ctx_controller:
            self.dl_request.log_ctx_controller.put_to_context("conn_id", conn_id)

        conn = await self.dl_request.us_manager.get_by_id(
            conn_id,
            ConnectionBase,
            context_name="connection",
        )
        assert isinstance(conn, ConnectionBase)

        self._enrich_logging_context_with_conn(conn)

        if not isinstance(conn, RawSqlLevelConnectionMixin):
            raise UnexpectedUSEntryType("Expecting a subselect-compatible connection")

        need_permission_on_entry(conn, USPermissionKind.execute)
        return conn_id, conn

    def _enrich_logging_context_with_conn(self, conn: ConnectionBase) -> None:
        ctrl = self.dl_request.log_ctx_controller
        if not ctrl:
            return
        ctrl.put_to_context("conn_type", conn.conn_type.name)
        ce_cls = self.dl_request.services_registry.get_conn_executor_factory().get_async_conn_executor_cls(conn)
        ctrl.put_to_context("conn_exec_cls", ce_cls.__qualname__)

    @staticmethod
    def _sql_json_default(value: Any) -> TJSONLike:
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
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return str(value)
        raise TypeError(f"Unexpected value type in SQL result serialization: {type(value)!r}")

    @classmethod
    def _json_sql_response(cls, payload: Any) -> web.Response:
        body = json.dumps(payload, default=cls._sql_json_default).encode()
        return web.Response(body=body, content_type="application/json")
