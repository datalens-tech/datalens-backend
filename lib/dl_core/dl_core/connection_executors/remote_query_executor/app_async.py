from __future__ import annotations

import argparse
import asyncio
import ipaddress
import logging
import pickle
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    Type,
    Union,
)

import aiodns
from aiohttp import web
from aiohttp.typedefs import Handler

from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.server_header import ServerHeader
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_app_tools.profiling_base import GenericProfiler
from dl_configs.env_var_definitions import (
    jaeger_service_name_env_aware,
    use_jaeger_tracer,
)
from dl_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import (
    AsyncDBAdapter,
    AsyncDirectDBAdapter,
)
from dl_core.connection_executors.adapters.async_adapters_sync_wrapper import AsyncWrapperForSyncAdapter
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.constants import (
    HEADER_BODY_SIGNATURE,
    HEADER_REQUEST_ID,
    HEADER_USE_JSON_SERIALIZER,
)
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
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
from dl_core.connection_executors.remote_query_executor.error_handler_rqe import RQEErrorHandler
from dl_core.connection_executors.remote_query_executor.settings import RQESettings
from dl_core.enums import RQEEventType
from dl_core.exc import SourceTimeout
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)
from dl_core.logging_config import configure_logging
from dl_dashsql.typed_query.query_serialization import get_typed_query_serializer
from dl_dashsql.typed_query.result_serialization import get_typed_query_result_serializer
from dl_model_tools.serialization import safe_dumps
from dl_utils.aio import ContextVarExecutor


if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO


LOGGER = logging.getLogger(__name__)


class BaseView(web.View):
    @property
    def tpe(self) -> ContextVarExecutor:
        return self.request.app["tpe"]

    @property
    def forbid_private_addr(self) -> bool:
        return self.request.app["forbid_private_addr"]


def adapter_factory(
    target_conn_dto: ConnTargetDTO,
    dba_cls: Type[CommonBaseDirectAdapter],
    req_ctx_info: DBAdapterScopedRCI,
    tpe: ContextVarExecutor,
) -> AsyncDBAdapter:
    default_chunk_size = DEFAULT_CHUNK_SIZE

    if issubclass(dba_cls, SyncDirectDBAdapter):
        sync_dba = dba_cls.create(
            target_dto=target_conn_dto, req_ctx_info=req_ctx_info, default_chunk_size=default_chunk_size
        )

        return AsyncWrapperForSyncAdapter(
            tpe=tpe,
            sync_adapter=sync_dba,
        )

    elif issubclass(dba_cls, AsyncDirectDBAdapter):
        return dba_cls.create(
            target_dto=target_conn_dto, req_ctx_info=req_ctx_info, default_chunk_size=default_chunk_size
        )

    else:
        raise ValueError(f"DBA class {dba_cls} is not supported in EQQ")


class PingView(BaseView):
    async def get(self) -> web.Response:
        return web.json_response(
            dict(
                result="PONG",
            )
        )


class ActionHandlingView(BaseView):
    async def get_action(self) -> act.RemoteDBAdapterAction:
        raw_body = await self.request.json()
        action = ActionSerializer().deserialize_action(raw_body, allowed_dba_classes=SUPPORTED_ADAPTER_CLS)  # type: ignore  # 2024-01-30 # TODO: Argument "allowed_dba_classes" to "deserialize_action" of "ActionSerializer" has incompatible type "set[type[CommonBaseDirectAdapter[Any]]]"; expected "frozenset[type[CommonBaseDirectAdapter[Any]]]"  [arg-type]
        return action

    @staticmethod
    def serialize_event(event: RQEEventType, data: Any) -> bytes:
        return pickle.dumps((event.value, data))

    async def handle_query_action(
        self,
        dba: AsyncDBAdapter,
        dba_query: DBAdapterQuery,
    ) -> Union[web.StreamResponse, web.Response]:
        try:
            result = await dba.execute(dba_query)
        except Exception:
            LOGGER.exception("Exception during execution")
            raise
        else:
            response = web.StreamResponse()
            response.enable_chunked_encoding()
            # TODO FIX: Use schema/custom JSON encoder

            await response.prepare(self.request)
            # After this point, `error_handling_middleware` does not work, so
            # send errors into the events stream:
            try:
                await response.write(self.serialize_event(RQEEventType.raw_cursor_info, result.raw_cursor_info))

                async for raw_chunk in result.raw_chunk_generator:
                    await response.write(self.serialize_event(RQEEventType.raw_chunk, raw_chunk))

            except Exception as err:
                await response.write(
                    self.serialize_event(RQEEventType.error_dump, ActionSerializer().serialize_exc(err))
                )
            # Proper chunked close should also indicate this, but making an explicit end-of-stream at this
            await response.write(self.serialize_event(RQEEventType.finished, None))

        return response

    async def handle_non_stream_query_action(
        self,
        dba: AsyncDBAdapter,
        dba_query: DBAdapterQuery,
    ) -> web.Response:
        try:
            result = await dba.execute(dba_query)
        except Exception:
            LOGGER.exception("Exception during execution")
            raise

        events: list[tuple[str, Any]] = [(RQEEventType.raw_cursor_info.value, result.raw_cursor_info)]
        async for raw_chunk in result.raw_chunk_generator:
            events.append((RQEEventType.raw_chunk.value, raw_chunk))
        events.append((RQEEventType.finished.value, None))

        with GenericProfiler("async_qe_serialization"):
            if self.request.headers.get(HEADER_USE_JSON_SERIALIZER) == "1":
                response = web.json_response(events, dumps=safe_dumps)
            else:
                response = web.Response(body=pickle.dumps(events))

        return response

    async def execute_non_streamed_action(
        self,
        dba: AsyncDBAdapter,
        action: act.NonStreamAction,
    ) -> ResponseTypes:
        if isinstance(action, act.ActionTest):
            await dba.test()
            return None

        elif isinstance(action, act.ActionGetDBVersion):
            return await dba.get_db_version(db_ident=action.db_ident)

        elif isinstance(action, act.ActionGetSchemaNames):
            return await dba.get_schema_names(db_ident=action.db_ident)

        elif isinstance(action, act.ActionGetTables):
            return await dba.get_tables(schema_ident=action.schema_ident)  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "list[TableIdent]", expected "RawSchemaInfo | list[str] | str | bool | int | None")  [return-value]

        elif isinstance(action, act.ActionGetTableInfo):
            return await dba.get_table_info(table_def=action.table_def, fetch_idx_info=action.fetch_idx_info)

        elif isinstance(action, act.ActionIsTableExists):
            return await dba.is_table_exists(table_ident=action.table_ident)

        elif isinstance(action, act.ActionExecuteTypedQuery):
            return await self._handle_execute_typed_query_action(dba=dba, action=action)

        else:
            raise NotImplementedError(f"Action {action} is not implemented in QE")

    async def _handle_execute_typed_query_action(
        self,
        dba: AsyncDBAdapter,
        action: act.ActionExecuteTypedQuery,
    ) -> str:
        tq_serializer = get_typed_query_serializer(query_type=action.query_type)
        typed_query = tq_serializer.deserialize(action.typed_query_str)
        tq_result = await dba.execute_typed_query(typed_query=typed_query)
        tq_result_serializer = get_typed_query_result_serializer(query_type=action.query_type)
        tq_result_str = tq_result_serializer.serialize(tq_result)
        return tq_result_str

    async def post(self) -> Union[web.Response, web.StreamResponse]:
        action = await self.get_action()
        LOGGER.info("Got QE action request: %s", action)

        # Creating adapter
        LOGGER.info("Creating DBA")
        adapter = adapter_factory(
            target_conn_dto=action.target_conn_dto,
            req_ctx_info=action.req_ctx_info,
            tpe=self.tpe,
            dba_cls=action.dba_cls,
        )
        async with adapter:
            LOGGER.info("DBA for action was created: %s", adapter)

            if self.forbid_private_addr:
                target_host = adapter.get_target_host()
                if target_host:
                    try:
                        ipaddress.ip_address(target_host)
                        host = target_host
                    except ValueError:
                        try:
                            resolver = aiodns.DNSResolver()
                            resp = await resolver.query(target_host, "A")
                            host = resp[0].host
                        except aiodns.error.DNSError:
                            host = None
                            LOGGER.warning("Cannot resolve host: %s", target_host, exc_info=True)
                    if host is None or ipaddress.ip_address(host).is_private:
                        await asyncio.sleep(30)
                        query = None
                        if isinstance(action, (act.ActionExecuteQuery, act.ActionNonStreamExecuteQuery)):
                            query = action.db_adapter_query.debug_compiled_query
                        raise SourceTimeout(db_message="Source timed out", query=query)

            if isinstance(action, act.ActionExecuteQuery):
                return await self.handle_query_action(adapter, action.db_adapter_query)

            if isinstance(action, act.ActionNonStreamExecuteQuery):
                return await self.handle_non_stream_query_action(adapter, action.db_adapter_query)

            if isinstance(action, act.NonStreamAction):
                result = await self.execute_non_streamed_action(adapter, action)
                resp_data = action.serialize_response(result)
                return web.json_response(resp_data)

            raise NotImplementedError(f"Action {action} is not implemented in QE")


def body_signature_validation_middleware(hmac_key: bytes) -> AIOHTTPMiddleware:
    @web.middleware
    async def actual_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
        if not hmac_key:  # do not consider an empty hmac key as valid.
            raise Exception("body_signature_validation_middleware: no hmac_key.")

        if request.method in ("HEAD", "OPTIONS", "GET"):
            return await handler(request)

        body_bytes = await request.read()
        expected_signature = get_hmac_hex_digest(body_bytes, secret_key=hmac_key)
        signature_str_from_header = request.headers.get(HEADER_BODY_SIGNATURE)

        if expected_signature != signature_str_from_header:
            raise web.HTTPForbidden(reason="Invalid signature")

        return await handler(request)

    return actual_middleware


def create_async_qe_app(hmac_key: bytes, forbid_private_addr: bool = False) -> web.Application:
    req_id_service = RequestId(
        header_name=HEADER_REQUEST_ID,
        accept_logging_ctx=True,
    )
    error_handler = RQEErrorHandler(
        sentry_app_name_tag=None,
    )
    app = web.Application(
        middlewares=[
            RequestBootstrap(
                req_id_service=req_id_service,
                error_handler=error_handler,
            ).middleware,
            # TODO FIX: Add profiling middleware.
            body_signature_validation_middleware(hmac_key=hmac_key),
        ]
    )
    app.on_response_prepare.append(req_id_service.on_response_prepare)
    ServerHeader("DataLens QE").add_signal_handlers(app)

    # TODO FIX: Close on app exit
    app["tpe"] = ContextVarExecutor()
    app["forbid_private_addr"] = forbid_private_addr

    app.router.add_route("get", "/ping", PingView)
    app.router.add_route("*", "/execute_action", ActionHandlingView)

    return app


def get_configured_qe_app() -> web.Application:
    configure_logging(
        app_name="rqe-async",
        app_prefix=None,  # not useful with `append_local_req_id=False`.
        use_jaeger_tracer=use_jaeger_tracer(),
        jaeger_service_name=jaeger_service_name_env_aware("bi-rqe-async"),
    )

    settings = load_settings_from_env_with_fallback(RQESettings)
    load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST))

    hmac_key = settings.RQE_SECRET_KEY
    if hmac_key is None:
        raise Exception("No `hmac_key` set.")

    return create_async_qe_app(hmac_key.encode(), forbid_private_addr=settings.FORBID_PRIVATE_ADDRESSES)


def async_qe_main() -> None:
    try:
        parser = argparse.ArgumentParser(description="Process some integers.")
        parser.add_argument("--host", type=str)
        parser.add_argument("--port", type=int)
        args = parser.parse_args()
    except Exception as err:
        LOGGER.exception("rqe-async args error: %r", err)
        sys.exit(-1)
        raise  # just-in-case

    app = get_configured_qe_app()
    web.run_app(app, host=args.host, port=args.port, print=LOGGER.info)
