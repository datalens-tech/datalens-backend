from __future__ import annotations

import json
import logging
import re
import ssl
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
)
import urllib.parse

import aiohttp
import attr

from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import ConnectionType
from dl_core.connection_executors.adapters.async_adapters_aiohttp import AiohttpDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawColumnInfo,
    RawSchemaInfo,
)
from dl_core.db.native_type import CommonNativeType
from dl_core.exc import (
    ConnectionConfigurationError,
    DatabaseQueryError,
)

from bi_connector_gsheets.core import gozora
from bi_connector_gsheets.core.constants import CONNECTION_TYPE_GSHEETS

if TYPE_CHECKING:
    from aiohttp.client import ClientResponse

    from dl_core.connection_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )


LOGGER = logging.getLogger(__name__)


class Uniqualizer:
    """Numerate non-unique strings"""

    template = "{}_{}"

    def __init__(self) -> None:
        self.counters = {}  # type: ignore  # TODO: fix

    def __call__(self, value: str) -> str:
        current = self.counters.get(value)
        if current is None:
            self.counters[value] = 1
            return value
        self.counters[value] = current + 1
        new_value = self.template.format(value, current)
        return self(new_value)


@attr.s
class GSheetsDefaultAdapter(AiohttpDBAdapter):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_GSHEETS
    _default_host: ClassVar[str] = "docs.google.com"
    _allowed_hosts: ClassVar[frozenset[str]] = frozenset({_default_host})
    _allowed_params: ClassVar[tuple[str, ...]] = ("gid", "sheet", "range")
    _example_url: ClassVar[
        str
    ] = "https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit#gid=1140182768"
    _example_url_message: ClassVar[str] = f"example: {_example_url!r}"

    def create_aiohttp_connector(self, ssl_context: Optional[ssl.SSLContext]) -> aiohttp.TCPConnector:
        return gozora.GoZoraTCPConnector(
            ssl_context=ssl_context,
        )

    @classmethod
    def process_url(cls, url: str, strict: bool = True) -> str:
        """
        >>> GSheetsDefaultAdapter.process_url('https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit#gid=235747210&range=C4:H12')
        'https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/gviz/tq?gid=235747210&range=C4%3AH12'
        >>> GSheetsDefaultAdapter.process_url('https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit?usp=sharing')
        'https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/gviz/tq'
        >>> GSheetsDefaultAdapter.process_url('https://notdocs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit?usp=sharing')
        Traceback (most recent call last):
        ...
        ValueError: Invalid host: 'notdocs.google.com'; should be 'docs.google.com'; example: 'https://docs.google.com/spreadsheets/d/1zRPTxxLOQ08n0_MReIBbouAbPFw6pJ4ibNSVdFkZ3gs/edit#gid=1140182768'
        """
        parts = urllib.parse.urlparse(url)
        if strict and parts.scheme not in ("http", "https"):
            raise ValueError(
                f"Invalid url scheme: {parts.scheme!r}, must be 'http' or 'https'; {cls._example_url_message}"
            )
        netloc_pieces = parts.netloc.split(":", 1)
        host = netloc_pieces[0]
        if len(netloc_pieces) == 2:
            port = netloc_pieces[1]
            if strict and port not in ("80", "443"):
                raise ValueError(f"Invalid port: {port!r}. Should be omitted or default; {cls._example_url_message}")
        if strict and host not in cls._allowed_hosts:
            raise ValueError(f"Invalid host: {host!r}; should be {cls._default_host!r}; {cls._example_url_message}")

        path = parts.path
        prefix = "/spreadsheets/d/"
        if strict and not path.startswith(prefix):
            raise ValueError(f"Invalid URL path prefix: {path!r}; should be {prefix!r}; {cls._example_url_message}")

        suffix = "/edit"
        if parts.path.endswith(suffix):
            path = path[: -len(suffix)]
        path = "/".join((path.rstrip("/"), "gviz/tq"))

        # Technically, `range` and `gid` shouldn't end up in the query;
        # practically, should be harmless.
        qs = urllib.parse.parse_qs(parts.query)
        fragm = urllib.parse.parse_qs(parts.fragment)

        params = {}
        for key in cls._allowed_params:
            value = fragm.get(key) or qs.get(key)
            if value:
                # gsheets interface itself takes the first value if multiple are present.
                params[key] = value[0]

        return urllib.parse.urlunparse((parts.scheme, parts.netloc, path, None, urllib.parse.urlencode(params), None))

    def _get_api_url(self) -> str:
        user_url = self._target_dto.sheets_url  # type: ignore  # TODO: fix
        try:
            api_url = self.process_url(user_url, strict=True)
        except ValueError as exc:
            raise ConnectionConfigurationError(f"Invalid URL in the connection: {exc.args[0]}")
        return api_url

    @generic_profiler_async("db-query")  # type: ignore  # TODO: fix
    async def _run_query(self, dba_q: DBAdapterQuery) -> ClientResponse:
        api_url = self._get_api_url()
        # TODO: ensure datetimes support
        query_text = self.compile_query_for_execution(dba_q.query)

        kwargs = {}
        if self._target_dto.use_gozora:  # type: ignore  # TODO: fix
            request_id = self._req_ctx_info.request_id
            kwargs.update(await gozora.prepare_aiohttp_kwargs(request_id))

        LOGGER.debug("Going to send query to GSheets, url: %s, query: %s", api_url, query_text)

        resp = await self._session.get(
            url=api_url,
            # SQL Query format note: identifiers quoted by backticks, no `from`.
            params=dict(tq=query_text),
            allow_redirects=False,
            headers={
                "X-DataSource-Auth": "true",  # switches from jsonp response to almost-json response
            },
            **kwargs,
        )
        return resp

    def make_exc(  # TODO:  Move to ErrorTransformer
        self,
        status_code: int,  # noqa
        err_body: str,
        debug_compiled_query: Optional[str] = None,
    ) -> DatabaseQueryError:
        exc_cls = DatabaseQueryError
        # TODO: exc_cls overrides
        return exc_cls(
            db_message=err_body,
            query=debug_compiled_query,
            orig=None,
            details={},
        )

    @staticmethod
    def _parse_response_body_data(body: str) -> dict:
        special_prefix = ")]}'\n"
        if body.startswith(special_prefix):
            body = body[len(special_prefix) :]
        try:
            data = json.loads(body)
        except ValueError:
            raise ValueError("not a valid JSON")
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        status = data.get("status")
        if status and status != "ok":
            raise ValueError("non-ok status")
        try:
            normalized_data = dict(
                # # All known fields are mentioned,
                # # only actually used fields are uncommented.
                # version=data.get('version'),
                # reqId=data.get('reqId'),
                # sig=data.get('sig'),
                # parsedNumHeaders=data['table']['parsedNumHeaders'],
                cols=[
                    dict(
                        id=col["id"],
                        label=col.get("label"),
                        type=col.get("type"),  # 'string'|'number'|'date'|'datetime'
                        # pattern=col.get('pattern'),
                    )
                    for col in data["table"]["cols"]
                ],
                rows=[
                    # item examples:
                    #     {'v': 135.0, 'f': '135'}
                    #     None
                    #     {'f': '1998-06-05', 'v': 'Date(1998,5,5)'}
                    #     {'f': '2021-03-12 13:14:15', 'v': 'Date(2021,2,12,13,14,15)'}
                    [item["v"] if item else None for item in row["c"]]
                    for row in data["table"]["rows"]
                ],
                # # Not currently used:
                # rows_formatted=[
                #     [item.get('f', item['v']) if item else None for item in row]
                #     for row in data['table']['rows']
                # ],
            )
        except (KeyError, TypeError, ValueError):
            raise ValueError("unexpected data structure")
        return normalized_data

    async def _parse_response_body(self, resp: ClientResponse, query: DBAdapterQuery) -> dict:
        # TODO later?: streamed parsing
        body = await resp.text()
        try:
            return self._parse_response_body_data(body)
        except ValueError as err:
            LOGGER.debug("Unexpected API response", extra={"db_message": body})
            body_piece = body[:1000] + ("…" if len(body) > 1000 else "")
            raise DatabaseQueryError(
                message=f"Unexpected API response body: {err.args[0]}",
                db_message=body_piece,
                query=query.debug_compiled_query,
                orig=None,
                details={},
            )

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        with self.wrap_execute_excs(query=query, stage="request"):
            # TODO FIX: Statuses/errors for retry
            resp = await self._run_query(query)

        if resp.status != 200:
            body = await resp.text()
            db_exc = self.make_exc(
                status_code=resp.status, err_body=body, debug_compiled_query=query.debug_compiled_query
            )
            raise db_exc

        rd = await self._parse_response_body(resp, query=query)

        async def chunk_gen(chunk_size=query.chunk_size or self._default_chunk_size):  # type: ignore  # TODO: fix
            data = rd["rows"]
            while data:
                chunk = data[:chunk_size]
                data = data[chunk_size:]
                yield chunk

        return AsyncRawExecutionResult(
            # Reminder: this `raw_cursor_info` gets (sometimes) serialized over RQE (async-ext).
            raw_cursor_info=dict(cols=rd["cols"]),
            raw_chunk_generator=chunk_gen(),
        )

    @classmethod
    def get_dialect_str(cls) -> str:
        return "gsheets"

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return None  # Not Applicable

    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError()

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError()

    @staticmethod
    def _normalize_col_title(title: str) -> str:
        return re.sub(r"[\[\]\{\}\\\n]", " ", title)

    async def get_table_info(
        self, table_def: Optional[TableDefinition] = None, fetch_idx_info: bool = False
    ) -> RawSchemaInfo:
        query_text = "select * limit 0"
        query_obj = DBAdapterQuery(query=query_text, debug_compiled_query=query_text)
        res = await self.execute(query_obj)
        res_cols = res.raw_cursor_info["cols"]

        uniqualizer = Uniqualizer()

        return RawSchemaInfo(
            columns=tuple(
                RawColumnInfo(
                    name=col["id"],
                    # Ensure the title is clean and unique.
                    title=uniqualizer(self._normalize_col_title(col["label"] or col["id"])),
                    nullable=True,
                    # column.get('nullable'),  # forced `True` for table-creation.  # to be deprecated entirely.
                    native_type=CommonNativeType(
                        conn_type=self.conn_type,
                        name=col["type"] or "string",
                        nullable=True,
                    ),
                )
                for col in res_cols
            )
        )

    async def test(self) -> None:
        await self.get_table_info()
