from __future__ import annotations

from datetime import datetime
import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Type,
)
from urllib.parse import (
    quote_plus,
    urljoin,
)

from aiohttp.client import ClientResponse
from aiohttp.helpers import BasicAuth
from aiohttp.web import HTTPBadRequest
import attr
import sqlalchemy as sa

from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import ConnectionType
from dl_core.connection_executors.adapters.adapters_base_sa_classic import (
    BaseClassicAdapter,
    ClassicSQLConnLineConstructor,
)
from dl_core.connection_executors.adapters.async_adapters_aiohttp import AiohttpDBAdapter
from dl_core.connection_executors.adapters.async_adapters_base import AsyncRawExecutionResult
from dl_core.db.native_type import GenericNativeType
from dl_core.exc import DatabaseQueryError
from dl_utils.utils import make_url

from dl_connector_promql.core.constants import CONNECTION_TYPE_PROMQL


if TYPE_CHECKING:
    from dl_constants.types import TBIChunksGen
    from dl_core.connection_executors.models.db_adapter_data import (
        DBAdapterQuery,
        RawSchemaInfo,
    )
    from dl_core.connection_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )

    from dl_connector_promql.core.target_dto import PromQLConnTargetDTO

LOGGER = logging.getLogger(__name__)


class PromQLConnLineConstructor(ClassicSQLConnLineConstructor["PromQLConnTargetDTO"]):
    def _get_dsn_params(
        self,
        safe_db_symbols: tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        return dict(
            dialect=self._dialect_name,
            user=quote_plus(self._target_dto.username) if self._target_dto.username is not None else None,
            passwd=quote_plus(self._target_dto.password) if self._target_dto.password is not None else None,
            host=quote_plus(self._target_dto.host),
            port=quote_plus(str(self._target_dto.port)),
            db_name=db_name or quote_plus(self._target_dto.db_name or "", safe="".join(safe_db_symbols)),
        )

    def _get_dsn_query_params(self) -> dict:
        return {
            "path": self._target_dto.path,
            "protocol": self._target_dto.protocol,
        }


@attr.s
class PromQLAdapter(BaseClassicAdapter["PromQLConnTargetDTO"]):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_PROMQL
    conn_line_constructor_type: ClassVar[Type[PromQLConnLineConstructor]] = PromQLConnLineConstructor

    _type_code_to_sa = {
        None: sa.TEXT,  # fallback
        "float64": sa.FLOAT,
        "string": sa.TEXT,
        "unix_timestamp": sa.DATETIME,
    }

    def _test(self) -> None:
        engine = self.get_db_engine(db_name="")
        connection = engine.raw_connection()
        try:
            connection.cli.test_connection()
        finally:
            connection.close()


class AsyncPromQLAdapter(AiohttpDBAdapter):
    conn_type: ClassVar[ConnectionType] = CONNECTION_TYPE_PROMQL

    _target_dto: PromQLConnTargetDTO = attr.ib()

    _url: str = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        self._url = make_url(
            protocol=self._target_dto.protocol,
            host=self._target_dto.host,
            port=self._target_dto.port,
            path=self._target_dto.path,
        )

    def get_session_auth(self) -> Optional[BasicAuth]:
        if self._target_dto.username and self._target_dto.password:
            return BasicAuth(
                login=self._target_dto.username,
                password=self._target_dto.password,
                encoding="utf-8",
            )
        return None

    async def _run_query(self, dba_query: DBAdapterQuery) -> ClientResponse:
        req_params = {"from", "to", "step"}
        conn_params = dba_query.connector_specific_params
        if conn_params is None or not req_params <= set(conn_params):
            db_exc = self.make_exc(
                status_code=HTTPBadRequest.status_code,
                err_body="'step', 'from', 'to' must be in parameters",
                debug_compiled_query=dba_query.debug_compiled_query,
            )
            raise db_exc

        for param in ("from", "to"):
            conn_param = conn_params[param]
            if isinstance(conn_param, datetime):
                conn_params[param] = int(conn_param.timestamp())

        query_text = self.compile_query_for_execution(dba_query.query)
        resp = await self._session.post(
            url=urljoin(self._url, "api/v1/query_range"),
            data={
                "query": query_text,
                "start": conn_params["from"],
                "end": conn_params["to"],
                "step": conn_params["step"],
            },
        )
        return resp

    @staticmethod
    def _parse_response_body_data(data: dict) -> dict:
        rows = []
        schema: list[tuple[str, str]] = []
        for chunk in data["result"]:
            chunk_schema = [("timestamp", "unix_timestamp"), ("value", "float64")] + [
                (label, "string") for label in chunk["metric"]
            ]

            if not schema:
                schema = chunk_schema
            elif set(schema) != set(chunk_schema):
                raise ValueError("Different schemas are not supported")

            label_values = [value for _, value in chunk["metric"].items()]
            for ts, v in chunk["values"]:
                row = [datetime.fromtimestamp(ts), float(v)] + label_values
                rows.append(row)

        return dict(rows=rows, schema=schema)

    async def _parse_response_body(self, resp: ClientResponse, dba_query: DBAdapterQuery) -> dict:
        data = await resp.json()
        try:
            return self._parse_response_body_data(data["data"])
        except ValueError as err:
            raise DatabaseQueryError(
                message=f"Unexpected API response body: {err.args[0]}",
                db_message=data["data"]["result"][:100],
                query=dba_query.debug_compiled_query,
            ) from err

    @staticmethod
    def make_exc(  # TODO:  Move to ErrorTransformer
        status_code: int,  # noqa
        err_body: str,
        debug_compiled_query: Optional[str] = None,
    ) -> DatabaseQueryError:
        exc_cls = DatabaseQueryError
        return exc_cls(
            db_message=err_body,
            query=debug_compiled_query,
            orig=None,
            details={},
        )

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, dba_query: DBAdapterQuery) -> AsyncRawExecutionResult:
        with self.wrap_execute_excs(query=dba_query, stage="request"):
            resp = await self._run_query(dba_query)

        if resp.status != 200:
            body = await resp.text()
            body_piece = body[:100] + ("…" if len(body) > 100 else "")  # TODO: depends on db ownership
            db_exc = self.make_exc(
                status_code=resp.status,
                err_body=body_piece,
                debug_compiled_query=dba_query.debug_compiled_query,
            )
            raise db_exc

        rd = await self._parse_response_body(resp, dba_query=dba_query)

        async def chunk_gen(
            chunk_size: int = dba_query.chunk_size or self._default_chunk_size,
        ) -> TBIChunksGen:
            data = rd["rows"]
            while data:
                chunk = data[:chunk_size]
                data = data[chunk_size:]
                yield chunk

        return AsyncRawExecutionResult(
            raw_cursor_info=dict(
                schema=rd["schema"],
                names=[name for name, _ in rd["schema"]],
                driver_types=[driver_type for _, driver_type in rd["schema"]],
                db_types=[self._promql_type_name_to_native_type(driver_type) for _, driver_type in rd["schema"]],
            ),
            raw_chunk_generator=chunk_gen(),
        )

    def _promql_type_name_to_native_type(self, type_name: str) -> GenericNativeType:
        return GenericNativeType.normalize_name_and_create(name=type_name)

    async def test(self) -> None:
        await self._session.get(
            url=urljoin(self._url, "-/ready"),
        )

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        raise NotImplementedError()

    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        raise NotImplementedError()

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError()

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError()
