from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import aiohttp
from aiohttp import (
    ClientTimeout,
    TCPConnector,
)
from aiohttp.client_exceptions import ServerTimeoutError
import attr

from dl_api_commons.crypto import get_hmac_hex_digest
from dl_api_commons.headers import (
    HEADER_LOGGING_CONTEXT,
    INTERNAL_HEADER_PROFILING_STACK,
)
from dl_api_commons.tracing import get_current_tracing_headers
from dl_app_tools import log
from dl_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler_async,
)
from dl_core import exc as common_exc
from dl_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import (
    AsyncDBAdapter,
    AsyncDirectDBAdapter,
    AsyncRawExecutionResult,
)
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.common import (
    RemoteQueryExecutorData,
    RQEExecuteRequestMode,
)
from dl_core.connection_executors.models.constants import (
    HEADER_BODY_SIGNATURE,
    HEADER_REQUEST_ID,
    HEADER_USE_NEW_QE_SERIALIZER,
)
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawSchemaInfo,
)
from dl_core.connection_executors.models.exc import QueryExecutorException
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_executors.qe_serializer import (
    ActionSerializer,
    ResponseTypes,
)
from dl_core.connection_executors.qe_serializer import dba_actions as dba_actions
from dl_core.connection_models.conn_options import ConnectOptions
from dl_core.enums import RQEEventType
from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryRaw,
    TypedQueryRawResult,
    TypedQueryResult,
)
from dl_dashsql.typed_query.query_serialization import get_typed_query_serializer
from dl_dashsql.typed_query.result_serialization import (
    DefaultTypedQueryRawResultSerializer,
    get_typed_query_result_serializer,
)
from dl_model_tools.msgpack import DLSafeMessagePackSerializer
from dl_utils.utils import make_url
from dl_core.connection_models import TableIdent

if TYPE_CHECKING:
    from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
    from dl_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
    )


LOGGER = logging.getLogger(__name__)

_RESP_TV = TypeVar("_RESP_TV", bound=ResponseTypes)


@attr.s()
class RemoteAsyncAdapter(AsyncDBAdapter):
    _target_dto: ConnTargetDTO = attr.ib()
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    _dba_cls: Type[CommonBaseDirectAdapter] = attr.ib()
    _rqe_data: RemoteQueryExecutorData = attr.ib()
    _conn_options: ConnectOptions = attr.ib(factory=ConnectOptions)
    _session: aiohttp.ClientSession = attr.ib(init=False)
    _force_async_rqe: bool = attr.ib(default=False)
    _serializer: ActionSerializer = attr.ib(init=False, factory=ActionSerializer)
    _use_sync_rqe: bool = attr.ib(init=False, default=None)

    DEFAULT_REL_PATH = "/execute_action"

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self._force_async_rqe:
            self._use_sync_rqe = False
        else:
            if issubclass(self._dba_cls, AsyncDirectDBAdapter):
                self._use_sync_rqe = False
            elif issubclass(self._dba_cls, SyncDirectDBAdapter):
                self._use_sync_rqe = True
            else:
                raise QueryExecutorException(f"Unexpected DBA class: {self._dba_cls}")

        self._session = aiohttp.ClientSession(
            # TODO CONSIDER: May be turn on keepalive on sync QE?
            # uWSGI at this moment fails on second request with keepalive=True
            connector=TCPConnector(force_close=True),
            timeout=ClientTimeout(
                total=(5 * 60 if (override := self._conn_options.rqe_total_timeout) is None else override),
                connect=5,
                sock_read=(600 if (override := self._conn_options.rqe_sock_read_timeout) is None else override),
                sock_connect=5,
            ),
        )

    # TODO FIX: Handle exceptions
    async def _make_request(
        self,
        req_obj: dba_actions.RemoteDBAdapterAction,
        rel_path: Optional[str] = None,
        use_new_qe_serializer: Optional[str] = None,
    ) -> aiohttp.ClientResponse:
        if rel_path is None:
            rel_path = self.DEFAULT_REL_PATH

        logging_context = log.context.get_log_context()
        serialized_context = json.dumps(logging_context)

        qe = self._rqe_data
        body_bytes = json.dumps(self._serializer.serialize_action(req_obj)).encode()
        signature = get_hmac_hex_digest(body_bytes, qe.hmac_key)

        tracing_headers = get_current_tracing_headers()

        headers = {
            HEADER_BODY_SIGNATURE: signature,
            HEADER_LOGGING_CONTEXT: serialized_context,
            INTERNAL_HEADER_PROFILING_STACK: GenericProfiler.get_current_stages_stack_str(),
            "Content-Type": "application/json",
            **tracing_headers,
        }
        if use_new_qe_serializer is not None:
            headers[HEADER_USE_NEW_QE_SERIALIZER] = use_new_qe_serializer
        if self._req_ctx_info.request_id:
            headers[HEADER_REQUEST_ID] = self._req_ctx_info.request_id

        url: str
        if self._use_sync_rqe:
            url = make_url(
                protocol=qe.sync_protocol,
                host=qe.sync_host,
                port=qe.sync_port,
                path=rel_path,
            )
        else:
            url = make_url(
                protocol=qe.async_protocol,
                host=qe.async_host,
                port=qe.async_port,
                path=rel_path,
            )

        LOGGER.info("Making request to QE: %s", url)
        try:
            resp = await self._session.post(
                url=url,
                headers=headers,
                data=body_bytes,
            )
        except ServerTimeoutError as err:
            # we want to distinguish between RQE and source timeouts; unfortunately,
            # aiohttp doesn't provide separate ConnectionTimeout and RequestTimeout
            # exceptions like requests does, so we should check it manually
            if str(err).startswith("Connection timeout"):
                raise

            query = (
                req_obj.db_adapter_query.debug_compiled_query
                if isinstance(req_obj, dba_actions.ActionExecuteQuery)
                else ""
            )
            raise common_exc.SourceTimeout(db_message="Source timed out", query=query) from err
        return resp

    @staticmethod
    async def _read_body_json(resp: aiohttp.ClientResponse) -> dict:
        try:
            body = await resp.read()
            try:
                return json.loads(body)
            except Exception as json_parsing_exc:
                raise QueryExecutorException("Response is not a valid JSON") from json_parsing_exc
        except Exception as unexpected_exc:
            raise QueryExecutorException("Unexpected exception during QE response parsing") from unexpected_exc
        finally:
            resp.release()

    def _parse_exception(self, body_json: dict) -> Exception:
        try:
            return self._serializer.deserialize_exc(body_json)
        except Exception as exc_deserialization_exc:
            raise QueryExecutorException("Unexpected error JSON schema") from exc_deserialization_exc

    async def _make_request_parse_response(
        self,
        req_obj: dba_actions.NonStreamAction[_RESP_TV],
        rel_path: Optional[str] = None,
    ) -> _RESP_TV:
        resp = await self._make_request(
            rel_path=rel_path,
            req_obj=req_obj,
        )

        async with resp:
            body_json = await self._read_body_json(resp)

        if resp.status != 200:
            exc = self._parse_exception(body_json)
            raise exc

        try:
            return req_obj.deserialize_response(body_json)
        except Exception as resp_deserialization_exc:
            raise QueryExecutorException("Unexpected response JSON schema") from resp_deserialization_exc

    @staticmethod
    def _parse_event(event: Any) -> Tuple[RQEEventType, Any]:
        if not isinstance(event, (list, tuple)):
            raise QueryExecutorException(f"QE parse: unexpected event type: {type(event)}")
        if len(event) != 2:
            raise QueryExecutorException(f"QE parse: event is not a pair: length={len(event)}")
        event_type_name, event_data = event
        if not isinstance(event_type_name, str):
            raise QueryExecutorException(f"QE parse: event_type is not a str: {type(event_type_name)}")
        try:
            event_type = RQEEventType[event_type_name]
        except KeyError:
            raise QueryExecutorException(f"QE parse: unknown event_type: {event_type_name!r}") from None

        return event_type, event_data

    async def _get_execution_result(
        self,
        events: typing.AsyncGenerator[Tuple[RQEEventType, Any], None],
    ) -> AsyncRawExecutionResult:
        ev_type, ev_data = await events.__anext__()
        if ev_type == RQEEventType.raw_cursor_info:
            raw_cursor_info = ev_data
        else:
            raise QueryExecutorException(f"QE parse: first event type is not 'raw_cursor_info': {ev_type}")

        # 3-layer iterable: generator of chunks, chunks is a list of rows, row
        # is a list of column values.
        async def data_generator() -> AsyncGenerator[Sequence[Sequence[Any]], None]:
            ev_type = None

            async for ev_type, ev_data in events:
                if ev_type == RQEEventType.raw_chunk:
                    chunk = ev_data
                    if not isinstance(chunk, (list, tuple)):
                        raise QueryExecutorException(f"QE parse: unexpected chunk type: {type(chunk)}")
                    yield chunk
                elif ev_type == RQEEventType.error_dump:
                    try:
                        exc = self._parse_exception(ev_data)
                    except Exception as e:
                        raise QueryExecutorException(f"QE parse: failed to parse an error event: {ev_data!r}") from e
                    raise exc
                elif ev_type == RQEEventType.finished:
                    return

            raise QueryExecutorException(f"QE parse: finish event was not received (last: {ev_type})")

        return AsyncRawExecutionResult(
            raw_cursor_info=raw_cursor_info,
            raw_chunk_generator=data_generator(),
        )

    # Here we have some async problem:
    #  "qe" stage will be finished before some nested stages in RQE (e.g. "qe/fetch")
    async def _execute_stream(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        resp = await self._make_request(
            dba_actions.ActionExecuteQuery(
                db_adapter_query=query,
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
            ),
        )

        if resp.status != 200:
            resp_body_json = await self._read_body_json(resp)
            exc = self._parse_exception(resp_body_json)
            raise exc

        async def event_gen() -> AsyncGenerator[Tuple[RQEEventType, Any], None]:
            buf = b""

            while True:
                try:
                    raw_chunk, end_of_chunk = await resp.content.readchunk()
                except asyncio.CancelledError as err:
                    raise common_exc.SourceTimeout(
                        db_message="Source timed out", query=query.debug_compiled_query
                    ) from err
                buf += raw_chunk

                # This isn't a very correct way, but it's hard to use pickle in async differently.
                if end_of_chunk and buf:
                    try:
                        event = pickle.loads(buf)
                    except Exception as err:
                        raise QueryExecutorException("QE parse: failed to unpickle") from err

                    yield self._parse_event(event)
                    buf = b""

                if raw_chunk == b"" and not end_of_chunk:
                    return

        events = event_gen()
        return await self._get_execution_result(events)

    async def _execute_non_stream(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        use_new_qe_serializer = os.environ.get("USE_NEW_QE_SERIALIZER")
        response = await self._make_request(
            dba_actions.ActionNonStreamExecuteQuery(
                db_adapter_query=query,
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
            ),
            use_new_qe_serializer=use_new_qe_serializer,
        )

        if response.status != 200:
            resp_body_json = await self._read_body_json(response)
            exc = self._parse_exception(resp_body_json)
            raise exc

        raw_data = await response.read()
        raw_events: Any
        with GenericProfiler("qe_deserialization"):
            if use_new_qe_serializer == "1":
                serializer = DLSafeMessagePackSerializer()
                raw_events = serializer.loads(raw_data)
            else:
                raw_events = pickle.loads(raw_data)

        async def event_gen() -> AsyncGenerator[Tuple[RQEEventType, Any], None]:
            for raw_event in raw_events:
                yield self._parse_event(raw_event)

        events = event_gen()
        return await self._get_execution_result(events)

    @generic_profiler_async("qe")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        if self._rqe_data.execute_request_mode == RQEExecuteRequestMode.STREAM:
            return await self._execute_stream(query)

        return await self._execute_non_stream(query)

    def get_target_host(self) -> Optional[str]:
        return self._target_dto.get_effective_host()

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return await self._make_request_parse_response(
            dba_actions.ActionGetDBVersion(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                db_ident=db_ident,
            ),
        )

    async def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        return await self._make_request_parse_response(
            dba_actions.ActionGetSchemaNames(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                db_ident=db_ident,
            ),
        )

    async def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        return await self._make_request_parse_response(  # type: ignore  # TODO: fix
            dba_actions.ActionGetTables(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                schema_ident=schema_ident,
            ),
        )

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        return await self._make_request_parse_response(
            dba_actions.ActionGetTableInfo(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                table_def=table_def,
                fetch_idx_info=fetch_idx_info,
            ),
        )

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        # table_ident = TableIdent(db_name='test_mysql_catalog', schema_name='test_data', table_name='table_ueopabpvy53rqgtrqqyeos')
        return await self._make_request_parse_response(
            dba_actions.ActionIsTableExists(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                table_ident=table_ident,
            ),
        )

    async def test(self) -> None:
        return await self._make_request_parse_response(
            dba_actions.ActionTest(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
            ),
        )

    async def execute_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        tq_serializer = get_typed_query_serializer(query_type=typed_query.query_type)
        typed_query_str = tq_serializer.serialize(typed_query)
        tq_result_str = await self._make_request_parse_response(
            dba_actions.ActionExecuteTypedQuery(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                query_type=typed_query.query_type,
                typed_query_str=typed_query_str,
            ),
        )
        tq_result_serializer = get_typed_query_result_serializer(query_type=typed_query.query_type)
        tq_result = tq_result_serializer.deserialize(tq_result_str)
        return tq_result

    async def execute_typed_query_raw(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        tq_serializer = get_typed_query_serializer(query_type=typed_query_raw.query_type)
        typed_query_str = tq_serializer.serialize(typed_query_raw)
        tq_result_str = await self._make_request_parse_response(
            dba_actions.ActionExecuteTypedQueryRaw(
                target_conn_dto=self._target_dto,
                dba_cls=self._dba_cls,
                req_ctx_info=self._req_ctx_info,
                query_type=typed_query_raw.query_type,
                typed_query_str=typed_query_str,
            ),
        )
        tq_result_serializer = DefaultTypedQueryRawResultSerializer()
        tq_result = tq_result_serializer.deserialize(tq_result_str)
        return tq_result

    async def close(self) -> None:
        await self._session.close()
