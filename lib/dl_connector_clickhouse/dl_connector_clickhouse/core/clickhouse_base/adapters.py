from __future__ import annotations

import contextlib
import json
import logging
import ssl
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    ClassVar,
    ContextManager,
    Generator,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from aiohttp import BasicAuth
from aiohttp.client import (
    ClientResponse,
    ClientTimeout,
)
import attr
from clickhouse_sqlalchemy import exceptions as ch_exc
from clickhouse_sqlalchemy import types as ch_types
from clickhouse_sqlalchemy.drivers.http.transport import _get_type  # noqa
from clickhouse_sqlalchemy.parsers.jsoncompact import JSONCompactChunksParser
import requests
import sqlalchemy as sa
from sqlalchemy.sql.type_api import TypeEngine

from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import IndexKind
from dl_core import exc
from dl_core.connection_executors.adapters.adapter_actions.typed_query import (
    AsyncTypedQueryAdapterAction,
    AsyncTypedQueryAdapterActionViaStandardExecute,
    TypedQueryToDBAQueryConverter,
)
from dl_core.connection_executors.adapters.adapters_base_sa_classic import (
    BaseClassicAdapter,
    ClassicSQLConnLineConstructor,
)
from dl_core.connection_executors.adapters.async_adapters_aiohttp import AiohttpDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.connection_executors.adapters.common_base import get_dialect_string
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawColumnInfo,
    RawIndexInfo,
    RawSchemaInfo,
)
from dl_core.connection_models import (
    SATextTableDefinition,
    TableDefinition,
    TableIdent,
)
from dl_core.connectors.base.error_handling import ExceptionMaker
from dl_core.connectors.base.error_transformer import (
    DBExcKWArgs,
    ExceptionInfo,
    GeneratedException,
    make_default_transformer_with_custom_rules,
)
from dl_core.connectors.ssl_common.adapter import BaseSSLCertAdapter
from dl_core.db.conversion_base import get_type_transformer
from dl_core.db.native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    GenericNativeType,
    norm_native_type,
)
from dl_dashsql.formatting.placeholder_dbapi import DBAPIQueryFormatterFactory
from dl_dashsql.registry import get_dash_sql_param_literalizer
from dl_utils.utils import make_url

from dl_connector_clickhouse.core.clickhouse_base.ch_commons import (
    ClickHouseBaseUtils,
    ClickHouseUtils,
    get_ch_settings,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.exc import CHRowTooLarge


if TYPE_CHECKING:
    from dl_constants.types import TBIChunksGen
    from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
    from dl_core.connection_models.common_models import SchemaIdent

    from dl_connector_clickhouse.core.clickhouse_base.target_dto import (  # noqa: F401
        BaseClickHouseConnTargetDTO,
        ClickHouseConnTargetDTO,
    )

LOGGER = logging.getLogger(__name__)

_DBA_TV = TypeVar("_DBA_TV", bound="BaseAsyncClickHouseAdapter")
_TARGET_DTO_TV = TypeVar("_TARGET_DTO_TV", bound="BaseClickHouseConnTargetDTO")


@attr.s()
class BaseClickHouseConnLineConstructor(ClassicSQLConnLineConstructor[_TARGET_DTO_TV]):
    _converted_headers: dict[str, str] = attr.ib()

    def _get_dsn_query_params(self) -> dict:
        # TODO FIX: Move to utils
        connect_timeout = self._target_dto.connect_timeout if self._target_dto.connect_timeout is not None else 1
        # TODO FIX: Move to utils
        # TODO CONSIDER: May be another constant?
        total_timeout = self._target_dto.total_timeout if self._target_dto.total_timeout is not None else 290

        return {
            **dict(
                protocol=self._target_dto.protocol,
                endpoint=self._target_dto.endpoint,
                timeout=total_timeout,
                connect_timeout=connect_timeout,
                stream=True,
            ),
            **self._converted_headers,
        }


class SyncDBAClickHouseExceptionMaker(ExceptionMaker):
    # FIXME: Move customization to ErrorTransformer subclass instead of ExceptionMaker

    ch_utils: ClassVar[Type[ClickHouseBaseUtils]] = ClickHouseBaseUtils

    def make_exc_info(
        self,
        wrapper_exc: Exception = GeneratedException(),
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        exc_info = super().make_exc_info(
            wrapper_exc=wrapper_exc,
            orig_exc=orig_exc,
            debug_compiled_query=debug_compiled_query,
            message=message,
        )

        ch_exc_cls: Optional[Type[exc.DatabaseQueryError]] = None
        if isinstance(wrapper_exc, ch_exc.DatabaseException):
            # Special case for ClickHouse
            # try to differentiate errors by error code
            db_msg = exc_info.exc_kwargs["db_message"]
            if db_msg:
                try:
                    # TODO: Temporary, will be fixed for sync adapter later
                    ch_exc_cls_with_params = self.ch_utils.get_exc_class(db_msg)
                    if ch_exc_cls_with_params:
                        ch_exc_cls, _ = ch_exc_cls_with_params
                        ch_exc_cls = ch_exc_cls or exc_info.exc_cls
                except Exception:  # noqa
                    LOGGER.info("Can not parse ClickHouse error message")

        if ch_exc_cls:
            exc_info = exc_info.clone(exc_cls=ch_exc_cls)

        elif isinstance(orig_exc, requests.exceptions.ReadTimeout):
            LOGGER.info("ClickHouse timed out")
            exc_info = exc_info.clone(exc_cls=exc.SourceTimeout)

        elif isinstance(orig_exc, requests.exceptions.ConnectionError):
            exc_info = exc_info.clone(exc_cls=exc.SourceConnectError)

        return exc_info


@attr.s
class BaseClickHouseAdapter(BaseClassicAdapter["BaseClickHouseConnTargetDTO"], BaseSSLCertAdapter):
    allow_sa_text_as_columns_source = True
    ch_utils: ClassVar[Type[ClickHouseBaseUtils]] = ClickHouseBaseUtils
    conn_line_constructor_type: ClassVar[Type[BaseClickHouseConnLineConstructor]] = BaseClickHouseConnLineConstructor

    _dt_with_system_tz = True

    def _make_exception_maker(self) -> ExceptionMaker:
        return SyncDBAClickHouseExceptionMaker(
            error_transformer=make_default_transformer_with_custom_rules(),
            extra_exception_classes=(ch_exc.DatabaseException,),
        )

    def _get_dsn_params_from_headers(self) -> dict[str, str]:
        return self._convert_headers_to_dsn_params(self.ch_utils.get_context_headers(self._req_ctx_info))

    def get_conn_line(self, db_name: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> str:
        return self.conn_line_constructor_type(
            dsn_template=self.dsn_template,
            dialect_name=get_dialect_string(self.conn_type),
            target_dto=self._target_dto,
            converted_headers=self._get_dsn_params_from_headers(),
        ).make_conn_line(db_name=db_name, params=params)

    # TODO FIX: Move to utils
    def get_ch_settings(self) -> dict:
        return get_ch_settings(
            max_execution_time=self._target_dto.max_execution_time,
            read_only_level=None,
            insert_quorum=self._target_dto.insert_quorum,
            insert_quorum_timeout=self._target_dto.insert_quorum_timeout,
            output_format_json_quote_denormals=1,
        )

    def get_connect_args(self) -> dict:
        args = {
            **super().get_connect_args(),
            # Hard-code server version
            #  to prevent querying in on each engine instantiation against CH server (default dialect behaviour).
            #  Dialect at this moment does not depends on CH version, so we can hard code it.
            "server_version": "19.16.2.2",
            "ch_settings": self.get_ch_settings(),
            "format": "JSONCompact",
        }

        if self._target_dto.secure:
            args["verify"] = self.get_ssl_cert_path(self._target_dto.ssl_ca)

        return args

    @contextlib.contextmanager
    def execution_context(self) -> Generator[None, None, None]:
        contexts: list[ContextManager[None]] = [super().execution_context()]

        if self._target_dto.ssl_ca:
            contexts.append(self.ssl_cert_context(self._target_dto.ssl_ca))

        with contextlib.ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            try:
                yield
            finally:
                stack.close()

    @staticmethod
    def _convert_headers_to_dsn_params(headers: dict[str, str]) -> dict[str, str]:
        return {f"header__{h_name}": h_val for h_name, h_val in headers.items()}

    # TODO CONSIDER: Do we really need to overwrite native type???
    def normalize_sa_col_type(self, sa_col_type: TypeEngine) -> TypeEngine:
        if isinstance(sa_col_type, ch_types.Decimal):
            return sa.Float()
        elif isinstance(sa_col_type, (ch_types.Enum8, ch_types.Enum16)):
            return sa.String()

        return super().normalize_sa_col_type(sa_col_type)

    def _test(self) -> None:
        self.execute(DBAdapterQuery("select 1"))

    def _get_subselect_table_info(self, subquery: SATextTableDefinition) -> RawSchemaInfo:
        # `describe table {table_func}/{subselect}` also works in CH.
        return RawSchemaInfo(columns=self._get_raw_columns_info(subquery))

    def _get_raw_columns_info(self, table_def: TableDefinition) -> tuple[RawColumnInfo, ...]:
        table_columns = self._get_sa_table_columns(table_def)
        db_name = table_def.db_name if isinstance(table_def, TableIdent) else None
        db_engine = self.get_db_engine(db_name)
        if self._dt_with_system_tz:
            system_tz = db_engine.scalar(sa.text("select timezone()"))

        def make_native_type(col):  # type: ignore  # TODO: fix
            col_type = col["type"]
            col_type = self.normalize_sa_col_type(col_type)

            nullable = col.get("nullable", True)
            lowcardinality = col.get("lowcardinality", False)
            is_array = False

            if isinstance(col_type, ch_types.Array):
                is_array = True
                col_type = col_type.item_type
            if isinstance(col_type, ch_types.LowCardinality):
                lowcardinality = True
                col_type = col_type.nested_type
            if isinstance(col_type, ch_types.Nullable):
                nullable = True
                col_type = col_type.nested_type

            explicit_timezone = True
            if self._dt_with_system_tz:
                if isinstance(col_type, ch_types.DateTime) and not isinstance(col_type, ch_types.DateTimeWithTZ):
                    explicit_timezone = False
                    col_type = ch_types.DateTimeWithTZ(system_tz)
                elif isinstance(col_type, ch_types.DateTime64) and not isinstance(col_type, ch_types.DateTime64WithTZ):
                    explicit_timezone = False
                    col_type = ch_types.DateTime64WithTZ(col_type.precision, system_tz)

            if is_array:
                name = norm_native_type(ch_types.Array(col_type))
            else:
                name = norm_native_type(col_type)

            if isinstance(col_type, ch_types.DateTimeWithTZ):
                return ClickHouseDateTimeWithTZNativeType(
                    name=name,  # type: ignore  # TODO: fix
                    nullable=nullable,
                    lowcardinality=lowcardinality,
                    timezone_name=col_type.tz,
                    explicit_timezone=explicit_timezone,
                )
            if isinstance(col_type, ch_types.DateTime64WithTZ):
                return ClickHouseDateTime64WithTZNativeType(
                    name=name,  # type: ignore  # TODO: fix
                    nullable=nullable,
                    lowcardinality=lowcardinality,
                    precision=col_type.precision,
                    timezone_name=col_type.tz,
                    explicit_timezone=explicit_timezone,
                )
            if isinstance(col_type, ch_types.DateTime64):
                return ClickHouseDateTime64NativeType(
                    name=name,  # type: ignore  # TODO: fix
                    nullable=nullable,
                    lowcardinality=lowcardinality,
                    precision=col_type.precision,
                )
            return ClickHouseNativeType(
                name=name,  # type: ignore  # TODO: fix
                nullable=nullable,
                lowcardinality=lowcardinality,
            )

        return tuple(
            RawColumnInfo(
                name=column["name"],
                title=column.get("title"),
                nullable=True,
                # column.get('nullable'),  # forced `True` for table-creation.  # to be deprecated entirely.
                native_type=make_native_type(column),
            )
            for column in table_columns
        )


class ClickHouseAdapter(BaseClickHouseAdapter):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    warn_on_default_db_name_override = False
    ch_utils = ClickHouseUtils

    def get_default_db_name(self) -> Optional[str]:
        return self._target_dto.db_name or "system"

    def _get_table_indexes(self, table_ident: TableIdent) -> tuple[RawIndexInfo, ...]:
        common_indexes = super()._get_table_indexes(table_ident)
        result = self.execute(
            DBAdapterQuery(
                query=sa.select(
                    [sa.column("sorting_key")],
                )
                .select_from(sa.table("tables"))
                .where(
                    sa.and_(
                        sa.column("name") == table_ident.table_name,
                        sa.column("database") == self.get_db_name_for_query(table_ident.db_name),
                    )
                ),
                db_name="system",
            )
        ).get_all()
        assert len(result) == 1, "Number of row for table name "

        ch_specific_idx_list = []
        sorting_key_str = result[0][0]

        if sorting_key_str and sorting_key_str.strip():
            sorting_key = tuple(col_name.strip() for col_name in sorting_key_str.split(","))
            ch_specific_idx_list.append(
                RawIndexInfo(
                    columns=sorting_key,
                    unique=False,
                    kind=IndexKind.table_sorting,
                )
            )

        return common_indexes + tuple(ch_specific_idx_list)


class AsyncDBAClickHouseExceptionMaker(ExceptionMaker):
    # FIXME: Move customization to ErrorTransformer subclass instead of ExceptionMaker

    ch_utils: ClassVar[Type[ClickHouseBaseUtils]] = ClickHouseBaseUtils

    def make_exc_info(
        self,
        wrapper_exc: Exception = GeneratedException(),
        orig_exc: Optional[Exception] = None,
        debug_compiled_query: Optional[str] = None,
        message: Optional[str] = None,
    ) -> ExceptionInfo:
        exc_cls: Type[exc.DatabaseQueryError]
        try:
            exc_cls, exc_kwargs = self.ch_utils.get_exc_class(message or "") or (exc.DatabaseQueryError, {})
        except Exception:  # noqa
            exc_cls, exc_kwargs = exc.DatabaseQueryError, {}

        exc_kwargs = exc_kwargs.copy()  # type: ignore  # TODO: Make sure this dict conforms to DBExcKWArgs restrictions
        if not exc_kwargs.get("db_message"):
            exc_kwargs["db_message"] = message or str(wrapper_exc)
        exc_inf = ExceptionInfo(exc_cls=exc_cls, exc_kwargs=exc_kwargs)  # type: ignore  # TODO: Same as above
        return exc_inf


@attr.s(kw_only=True)
class BaseAsyncClickHouseAdapter(AiohttpDBAdapter):
    ch_utils: ClassVar[Type[ClickHouseBaseUtils]] = ClickHouseBaseUtils

    _target_dto: BaseClickHouseConnTargetDTO = attr.ib()

    _url: str = attr.ib(init=False, default=None)

    def _make_exception_maker(self) -> ExceptionMaker:
        return AsyncDBAClickHouseExceptionMaker(
            error_transformer=make_default_transformer_with_custom_rules(),
        )

    def _make_async_typed_query_action(self) -> AsyncTypedQueryAdapterAction:
        literalizer = get_dash_sql_param_literalizer(backend_type=self.get_backend_type())
        type_transformer = get_type_transformer(conn_type=self.conn_type)
        return AsyncTypedQueryAdapterActionViaStandardExecute(
            async_adapter=self,
            query_converter=TypedQueryToDBAQueryConverter(
                literalizer=literalizer,
                query_formatter_factory=DBAPIQueryFormatterFactory(),
            ),
            type_transformer=type_transformer,
        )

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        self._url = make_url(
            protocol=self._target_dto.protocol,
            host=self._target_dto.host,
            port=self._target_dto.port,
            path=self._target_dto.endpoint,
        )

    def get_session_timeout(self) -> Optional[ClientTimeout]:  # type: ignore  # TODO: fix
        return ClientTimeout(connect=self._target_dto.connect_timeout, total=self._target_dto.total_timeout)

    def get_session_auth(self) -> Optional[BasicAuth]:
        return BasicAuth(
            login=self._target_dto.username,
            password=self._target_dto.password,
            encoding="utf-8",
        )

    def get_session_headers(self) -> dict[str, str]:
        return {
            **super().get_session_headers(),
            **self.ch_utils.get_context_headers(self._req_ctx_info),
        }

    def get_request_params(self, dba_q: DBAdapterQuery) -> dict[str, str]:
        read_only_level = None if dba_q.trusted_query else 2
        return dict(
            # TODO FIX: Move to utils
            database=dba_q.db_name or self._target_dto.db_name or "system",
            **get_ch_settings(
                read_only_level=read_only_level,
                max_execution_time=self._target_dto.max_execution_time,
                # doesn't matter until materializer uses async ch adapter
                insert_quorum=self._target_dto.insert_quorum,
                insert_quorum_timeout=self._target_dto.insert_quorum_timeout,
            ),
        )

    def _get_current_tracing_headers(self) -> dict[str, str]:
        return {}

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        return None

    # TODO FIX: Add logging from dl_core.connection_executors.adapters.sa_utils.CursorLogger
    async def _make_query(self, dba_q: DBAdapterQuery, mirroring_mode: bool = False) -> ClientResponse:
        query_str: str
        if isinstance(dba_q.query, str):
            _query_for_check = dba_q.query.lower()
            if not dba_q.trusted_query:
                assert _query_for_check.startswith(("select ", "select\n", "explain ", "explain\n"))
            query_str = dba_q.query
        else:
            query_str = dba_q.query.compile(dialect=self.get_dialect(), compile_kwargs={"literal_binds": True}).string
            # SA generates query for DBAPI, so mod is represented as `%%` so next hack is needed
            query_str = query_str % ()

        final_query = query_str + "\n" + "FORMAT JSONCompact"
        ch_params = self.get_request_params(dba_q)

        query_for_log = (
            dba_q.debug_compiled_query + "\n" + "FORMAT JSONCompact"
            if isinstance(dba_q.debug_compiled_query, str)
            else final_query
        )

        max_log_len = 1024 * 4
        LOGGER.info(
            "Going to send query to CH%s: %s%s",
            " (mirroring_mode=True)" if mirroring_mode else "",
            query_for_log[:max_log_len],
            "..." if len(query_for_log) > max_log_len else "",
            extra={
                "ch_query": query_for_log,
                "ch_url": self._url,
                "ch_params": json.dumps(ch_params),
                "event_code": "async_dba_ch_query_ready",
            },
        )

        tracing_headers = self._get_current_tracing_headers()

        resp = await self._session.post(
            url=self._url,
            params=ch_params,
            data=final_query.encode(),
            allow_redirects=False,
            headers={**tracing_headers},
            ssl_context=self._get_ssl_context(),
        )

        # CHYT rewrites incoming query_id so we log returned one instead of constructing own id
        LOGGER.info("Query has been sent to CH with query_id %s", resp.headers.get("X-ClickHouse-Query-Id"))

        return resp

    async def _parse_response_body(self, resp: ClientResponse) -> AsyncGenerator[tuple[str, Any], None]:
        parser = JSONCompactChunksParser()

        event = None
        data = None
        bytes_chunk: Optional[bytes] = None
        finish_sent = False

        async for bytes_chunk in resp.content.iter_chunked(self._http_read_chunk_size):
            parser.receive_data(bytes_chunk)

            while True:
                try:
                    event, data = parser.next_event()
                except parser.RowTooLarge:
                    raise CHRowTooLarge() from None
                if event == parser.parts.NEED_DATA:
                    break
                elif event == parser.parts.FINISHED:
                    if not finish_sent:
                        finish_sent = True
                        yield event, data
                    else:
                        # TODO FIX: Remove when JSONCompactChunksParser become able
                        #  to properly finalize remaining data in buffer after FINISH event
                        assert bytes_chunk is not None
                        LOGGER.info(
                            "Got duplicated FINISHED event from stream parser, chunk len=%s, truncated chunk: %r",
                            len(bytes_chunk),
                            bytes_chunk[:1024],
                        )

                    break
                else:
                    yield event, data

        else:
            if event != parser.parts.FINISHED:
                # TODO: return a sensible user-facing error in this case
                # (see a test in `bi-api/tests/db/result/test_error_handling.py`)
                decoded_chunk: Optional[str] = bytes_chunk.decode(errors="replace") if bytes_chunk is not None else None
                raise exc.SourceProtocolError(
                    debug_info=dict(msg="Unexpected last event", event=event, data=data, chunk=decoded_chunk),
                    # TODO: provide the actual query
                    query="",
                    # TODO?: show the chunk here for the user databases?
                    db_message="",
                )

        try:
            LOGGER.info(
                "Dumping clickhouse summary from headers",
                extra={"ch_summary": json.loads(resp.headers["X-ClickHouse-Summary"])},
            )
        except KeyError:
            LOGGER.warning("No clickhouse summary in headers")
        except Exception:
            LOGGER.warning("Failed to dump clickhouse summary", exc_info=True)

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        et = JSONCompactChunksParser.parts

        with self.wrap_execute_excs(query=query, stage="request"):
            # TODO FIX: Statuses for retry
            resp = await self._make_query(query)

        if resp.status != 200:
            bytes_body = await resp.read()
            body = bytes_body.decode(errors="replace")
            db_exc = self._exception_maker.make_exc(
                message=body,
                debug_compiled_query=query.debug_compiled_query,
            )
            raise db_exc

        # Primarily for DashSQL
        ch_resp_headers = {key: val for key, val in resp.headers.items() if key.lower().startswith("x-clickhouse-")}

        if query.is_ddl_dml_query:

            async def empty_chunk_gen() -> TBIChunksGen:
                return
                yield  # noqa

            return AsyncRawExecutionResult(
                raw_cursor_info=dict(clickhouse_headers=ch_resp_headers),
                raw_chunk_generator=empty_chunk_gen(),
            )

        def _safe_col_converter(col_conv: Callable[[str], Any], val: str) -> Any:
            try:
                return col_conv(val)
            except ValueError as err:
                raise exc.DataParseError(f"Cannot convert {val!r}", query=query.debug_compiled_query) from err

        events_generator = self._parse_response_body(resp)
        with self.wrap_execute_excs(query=query, stage="meta"):
            first_evt_type, first_evt_data = await events_generator.__anext__()
        assert first_evt_type == et.META
        if self._target_dto.disable_value_processing:
            row_converters = tuple(None for _ in first_evt_data)
        else:
            row_converters = tuple(_get_type(field["type"]) for field in first_evt_data)

        # TODO FIX: Here we ignore requested size of chunk
        # TODO FIX: Handle to large row exception
        async def chunk_gen() -> TBIChunksGen:
            expected_types: Sequence[Any] = (et.DATACHUNK, et.STATS)
            evt_type, evt_data = None, None

            with self.wrap_execute_excs(query=query, stage="rows"):
                async for evt_tuple in events_generator:
                    evt_type, evt_data = evt_tuple
                    if evt_type not in expected_types:
                        raise exc.SourceProtocolError()  # type: ignore  # TODO: fix

                    if evt_type == et.DATACHUNK:
                        yield tuple(
                            tuple(
                                _safe_col_converter(col_converter, val)  # type: ignore  # TODO: fix
                                if col_converter is not None
                                else val
                                for val, col_converter in zip(raw_row, row_converters, strict=True)
                            )
                            for raw_row in evt_data
                        )
                    elif evt_type == et.STATS:
                        LOGGER.info(
                            "Dumping clickhouse statistics from response",
                            extra=evt_data,  # statistics (elapsed, rows_read, bytes_read) + rows (num)
                        )
                        expected_types = (et.FINISHED,)
                    elif evt_type == et.FINISHED:
                        expected_types = ()

            if evt_type != et.FINISHED:
                # TODO: For user databases this should probably be passed to the user.
                raise exc.SourceProtocolError(  # type: ignore  # TODO: fix
                    debug_info=dict(msg="Unexpected last event", event=evt_type, data=evt_data)
                )

        return AsyncRawExecutionResult(
            # Reminder: this `raw_cursor_info` gets (sometimes) serialized over RQE (async-ext).
            # Primarily useful for DashSQL
            raw_cursor_info=dict(
                # Deprecating this in favor of `names` + `db_types`:
                columns=[{"name": col["name"], "clickhouse_type_name": col["type"]} for col in first_evt_data],
                # ...
                names=[col["name"] for col in first_evt_data],
                driver_types=[col["type"] for col in first_evt_data],
                db_types=[self._ch_type_name_to_native_type(col["type"]) for col in first_evt_data],
                clickhouse_headers=ch_resp_headers,
            ),
            # Data
            raw_chunk_generator=chunk_gen(),
        )

    def _ch_type_name_to_native_type(self, type_name: str) -> GenericNativeType:
        """A simplified CH type parser for some commonly useful cases"""
        type_pieces = type_name.lower().strip(")").split("(")
        if type_pieces and type_pieces[0] == "lowcardinality":
            type_pieces = type_pieces[1:]
        if type_pieces and type_pieces[0] == "nullable":
            type_pieces = type_pieces[1:]
        return GenericNativeType.normalize_name_and_create(
            name=type_pieces[0] if type_pieces else "",
        )

    async def test(self) -> None:
        await self.execute(DBAdapterQuery("select 1"))

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError()

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError()

    @classmethod
    def create(  # type: ignore  # TODO: fix
        cls: Type[_DBA_TV],
        target_dto: _TARGET_DTO_TV,
        req_ctx_info: DBAdapterScopedRCI,
        default_chunk_size: int,
    ) -> _DBA_TV:
        return cls(
            target_dto=target_dto,
            req_ctx_info=req_ctx_info,
            default_chunk_size=default_chunk_size,
        )


class AsyncClickHouseAdapter(BaseAsyncClickHouseAdapter):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    ch_utils = ClickHouseUtils

    def _get_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._target_dto.secure or self._target_dto.ssl_ca is None:
            return None

        return ssl.create_default_context(cadata=self._target_dto.ssl_ca)
