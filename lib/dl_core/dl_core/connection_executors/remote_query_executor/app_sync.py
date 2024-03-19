from __future__ import annotations

import logging
import pickle
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
    Tuple,
    Union,
)

import attr
import flask.views
from werkzeug.exceptions import (
    Forbidden,
    HTTPException,
)

from dl_api_commons.flask.middlewares.aio_event_loop_middleware import AIOEventLoopMiddleware
from dl_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware
from dl_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from dl_api_commons.flask.middlewares.request_id import RequestIDService
from dl_api_commons.flask.middlewares.tracing import TracingMiddleware
from dl_configs.env_var_definitions import (
    jaeger_service_name_env_aware,
    use_jaeger_tracer,
)
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_core import profiling_middleware
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.models.constants import HEADER_BODY_SIGNATURE
from dl_core.connection_executors.qe_serializer import (
    ActionSerializer,
    ResponseTypes,
)
from dl_core.connection_executors.qe_serializer import dba_actions as act
from dl_core.connection_executors.remote_query_executor.commons import (
    DEFAULT_CHUNK_SIZE,
    SUPPORTED_ADAPTER_CLS,
)
from dl_core.connection_executors.remote_query_executor.crypto import get_hmac_hex_digest
from dl_core.connection_executors.remote_query_executor.settings import RQESettings
from dl_core.enums import RQEEventType
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_core.logging_config import hook_configure_logging as _hook_configure_logging
from dl_dashsql.typed_query.query_serialization import get_typed_query_serializer
from dl_dashsql.typed_query.result_serialization import get_typed_query_result_serializer


if TYPE_CHECKING:
    from dl_core.connection_executors.adapters.adapters_base import DBAdapterQueryResult


LOGGER = logging.getLogger(__name__)


def chunked_wrap(iterable: Iterable[Union[bytes, str]]) -> Iterable[bytes]:
    """
    `Transfer-Encoding: Chunked` wrap.

    See also: https://rhodesmill.org/brandon/2013/chunked-wsgi/
    """
    for item in iterable:
        if not item:
            continue
        if isinstance(item, str):
            item = item.encode("utf-8")
        yield b"%x" % (len(item),)
        yield b"\r\n"
        yield item
        yield b"\r\n"
    yield b"0\r\n\r\n"


class ActionHandlingView(flask.views.View):
    methods = ["POST"]

    def get_action(self) -> act.RemoteDBAdapterAction:
        return ActionSerializer().deserialize_action(flask.request.json, allowed_dba_classes=SUPPORTED_ADAPTER_CLS)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "deserialize_action" of "ActionSerializer" has incompatible type "Any | None"; expected "dict[Any, Any]"  [arg-type]

    def execute_non_streamed_action(
        self,
        dba: SyncDirectDBAdapter,
        action: act.NonStreamAction,
    ) -> ResponseTypes:
        if isinstance(action, act.ActionTest):
            return dba.test()

        elif isinstance(action, act.ActionGetDBVersion):
            return dba.get_db_version(db_ident=action.db_ident)

        elif isinstance(action, act.ActionGetSchemaNames):
            return dba.get_schema_names(db_ident=action.db_ident)

        elif isinstance(action, act.ActionGetTables):
            return dba.get_tables(schema_ident=action.schema_ident)  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "list[TableIdent]", expected "RawSchemaInfo | list[str] | str | bool | int | None")  [return-value]

        elif isinstance(action, act.ActionGetTableInfo):
            return dba.get_table_info(table_def=action.table_def, fetch_idx_info=action.fetch_idx_info)

        elif isinstance(action, act.ActionIsTableExists):
            return dba.is_table_exists(table_ident=action.table_ident)

        elif isinstance(action, act.ActionExecuteTypedQuery):
            return self._handle_execute_typed_query_action(dba=dba, action=action)

        else:
            raise NotImplementedError(f"Action {action} is not implemented in QE")

    def _handle_execute_typed_query_action(
        self,
        dba: SyncDirectDBAdapter,
        action: act.ActionExecuteTypedQuery,
    ) -> str:
        tq_serializer = get_typed_query_serializer(query_type=action.query_type)
        typed_query = tq_serializer.deserialize(action.typed_query_str)
        tq_result = dba.execute_typed_query(typed_query=typed_query)
        tq_result_serializer = get_typed_query_result_serializer(query_type=action.query_type)
        tq_result_str = tq_result_serializer.serialize(tq_result)
        return tq_result_str

    @staticmethod
    def try_close_dba(dba: SyncDirectDBAdapter) -> None:
        try:
            dba.close()
        except Exception:
            LOGGER.exception("Exception during DBA closing")

    @staticmethod
    def serialize_event(event: RQEEventType, data: Any) -> bytes:
        return pickle.dumps((event.value, data))

    def response_events_gen(self, db_result: "DBAdapterQueryResult", dba: SyncDirectDBAdapter) -> Iterable[bytes]:
        try:
            yield self.serialize_event(RQEEventType.raw_cursor_info, db_result.cursor_info)

            for raw_chunk in db_result.data_chunks:
                yield self.serialize_event(RQEEventType.raw_chunk, raw_chunk)

        except Exception as err:
            yield self.serialize_event(RQEEventType.error_dump, ActionSerializer().serialize_exc(err))

        finally:
            dba.close()

        yield self.serialize_event(RQEEventType.finished, None)

    def execute_execute_action(
        self,
        action: act.ActionExecuteQuery,
    ) -> flask.Response:
        dba = self.create_dba_for_action(action)
        try:
            db_result = dba.execute(action.db_adapter_query)
        except Exception:
            # noinspection PyBroadException
            self.try_close_dba(dba)
            raise

        return flask.Response(
            chunked_wrap(self.response_events_gen(db_result=db_result, dba=dba)),
            headers={
                "Transfer-Encoding": "Chunked",
            },
        )

    def execute_non_stream_execute_action(
        self,
        action: act.ActionNonStreamExecuteQuery,
    ) -> flask.Response:
        dba = self.create_dba_for_action(action)

        try:
            db_result = dba.execute(action.db_adapter_query)
            events: list[tuple[str, Any]] = [(RQEEventType.raw_cursor_info.value, db_result.cursor_info)]
            for raw_chunk in db_result.data_chunks:
                events.append((RQEEventType.raw_chunk.value, raw_chunk))
            events.append((RQEEventType.finished.value, None))

            return flask.Response(response=pickle.dumps(events))
        except Exception:
            # noinspection PyBroadException
            self.try_close_dba(dba)
            raise

    @staticmethod
    def create_dba_for_action(action: act.RemoteDBAdapterAction) -> SyncDirectDBAdapter:
        LOGGER.info("Creating DBA")
        default_chunk_size = DEFAULT_CHUNK_SIZE
        dba_cls = action.dba_cls
        if not issubclass(dba_cls, SyncDirectDBAdapter):
            raise TypeError(f"Can not create DBA type '{dba_cls}' in sync RQE")

        dba = dba_cls.create(
            target_dto=action.target_conn_dto,
            req_ctx_info=action.req_ctx_info,
            default_chunk_size=default_chunk_size,
        )
        LOGGER.info("DBA for action was created: %s", dba)
        return dba

    def dispatch_request(self) -> flask.Response:
        action = self.get_action()
        LOGGER.info("Got QE action request: %s", action)

        if isinstance(action, act.ActionExecuteQuery):
            return self.execute_execute_action(action)

        if isinstance(action, act.ActionNonStreamExecuteQuery):
            return self.execute_non_stream_execute_action(action)

        if isinstance(action, act.NonStreamAction):
            dba = self.create_dba_for_action(action)
            try:
                result = self.execute_non_streamed_action(dba, action)
                return flask.jsonify(action.serialize_response(result))
            finally:
                dba.close()

        raise NotImplementedError(f"Action {action} is not implemented in QE")


@attr.s
class BodySignatureValidator:
    hmac_key: bytes = attr.ib()

    def validate_request_body(self) -> None:
        if flask.request.method in ("HEAD", "OPTIONS", "GET"):  # no body to validate.
            return

        # For import-test reasons, can't verify this when getting it;
        # but allowing requests when the key is empty is too dangerous.
        if not self.hmac_key:
            raise Exception("validate_request_body: no hmac_key")

        body_bytes = flask.request.get_data()
        expected_signature = get_hmac_hex_digest(body_bytes, secret_key=self.hmac_key)
        signature_str_from_header = flask.request.headers.get(HEADER_BODY_SIGNATURE)

        if expected_signature != signature_str_from_header:
            raise Forbidden("Invalid signature")

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self.validate_request_body)


def ping_view() -> flask.Response:
    return flask.jsonify(dict(result="PONG"))


def hook_init_logging(
    app: Any,
    app_name: str = "rqe-sync",
    jaeger_service_name: str = "bi-rqe-sync",
    app_prefix: Optional[str] = None,
    **kwargs: Any,
) -> None:
    return _hook_configure_logging(
        app=app,
        app_name=app_name,
        app_prefix=app_prefix,
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware(jaeger_service_name),
        **kwargs,
    )


def _handle_exception(err: Exception) -> Tuple[flask.Response, int]:
    LOGGER.exception("Exception occurred in sync RQE")
    if isinstance(err, HTTPException):
        return flask.jsonify(ActionSerializer().serialize_exc(err)), err.code or 500
    else:
        return flask.jsonify(ActionSerializer().serialize_exc(err)), 500


def create_sync_app() -> flask.Flask:
    settings = load_settings_from_env_with_fallback(RQESettings)
    hmac_key = settings.RQE_SECRET_KEY
    if hmac_key is None:
        raise Exception("No `hmac_key` set.")

    load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST))

    app = flask.Flask(__name__)
    TracingMiddleware(
        url_prefix_exclude=(
            "/ping",
            "/unistat",
            "/metrics",
        ),
    ).wrap_flask_app(app)
    ContextVarMiddleware().wrap_flask_app(app)
    AIOEventLoopMiddleware().wrap_flask_app(app)

    RequestLoggingContextControllerMiddleWare().set_up(app)
    RequestIDService(
        request_id_app_prefix=None,
        append_local_req_id=False,
        accept_logging_context=True,
    ).set_up(app)
    profiling_middleware.set_up(app, accept_outer_stages=True)
    BodySignatureValidator(
        hmac_key=hmac_key.encode(),
    ).set_up(app)

    app.add_url_rule("/ping", view_func=ping_view)
    app.add_url_rule("/execute_action", view_func=ActionHandlingView.as_view("execute_action"))

    app.errorhandler(Exception)(_handle_exception)

    return app
