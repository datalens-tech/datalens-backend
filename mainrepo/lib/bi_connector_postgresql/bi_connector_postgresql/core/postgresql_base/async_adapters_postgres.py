from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import datetime
import logging
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    ClassVar,
    Iterable,
    Optional,
    Type,
    TypeVar,
)
from urllib.parse import quote_plus

import asyncpg
import asyncpg.exceptions
import attr
from dateutil import parser as dateutil_parser
import sqlalchemy as sa

from bi_app_tools.profiling_base import generic_profiler_async
from bi_constants.types import (
    TBIChunksGen,
    TBIDataRow,
)
from bi_core import exc
from bi_core.connection_executors.adapters.adapters_base_sa_classic import ClassicSQLConnLineConstructor
from bi_core.connection_executors.adapters.async_adapters_base import (
    AsyncCache,
    AsyncDirectDBAdapter,
    AsyncRawExecutionResult,
)
from bi_core.connection_executors.adapters.mixins import (
    SATypeTransformer,
    WithAsyncGetDBVersion,
    WithDatabaseNameOverride,
)
from bi_core.connection_executors.adapters.sa_utils import make_debug_query
from bi_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawSchemaInfo,
)
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.connection_models import (
    DBIdent,
    TableDefinition,
)
from bi_core.connection_models.common_models import TableIdent
from bi_core.connectors.base.error_handling import ETBasedExceptionMaker
from bi_sqlalchemy_postgres import AsyncBIPGDialect
from bi_sqlalchemy_postgres.asyncpg import DBAPIMock

from bi_connector_postgresql.core.postgresql_base.adapters_base_postgres import (
    OID_KNOWLEDGE,
    PG_LIST_SOURCES_ALL_SCHEMAS_SQL,
    BasePostgresAdapter,
)
from bi_connector_postgresql.core.postgresql_base.error_transformer import make_async_pg_error_transformer
from bi_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO
from bi_connector_postgresql.core.postgresql_base.utils import compile_pg_query

if TYPE_CHECKING:
    from bi_core.connection_models.common_models import SchemaIdent

_DBA_ASYNC_POSTGRES_TV = TypeVar("_DBA_ASYNC_POSTGRES_TV", bound="AsyncPostgresAdapter")

LOGGER = logging.getLogger(__name__)


PG_LIST_SCHEMA_NAMES = """
SELECT nspname FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%'
ORDER BY nspname
"""

PG_LIST_TABLE_NAMES = """
SELECT c.relname FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :schema AND c.relkind in ('r', 'p')
"""

# https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/lib/sqlalchemy/dialects/postgresql/base.py#L3802
PG_LIST_VIEW_NAMES = """
SELECT c.relname FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :schema AND c.relkind IN 'v', 'm'
"""


# native SA asyncpg dialect is beta now
# we can use it after release
# look at https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html
# right now we have to implement some logic in our code
@attr.s(cmp=False, kw_only=True)
class AsyncPostgresAdapter(
    WithAsyncGetDBVersion,
    WithDatabaseNameOverride,
    AsyncDirectDBAdapter,
    BasePostgresAdapter,
    SATypeTransformer,
    ETBasedExceptionMaker,
):
    dsn_template: ClassVar[str] = "{dialect}://{user}:{passwd}@{host}:{port}/{db_name}"
    _default_chunk_size: int = attr.ib()
    _target_dto: PostgresConnTargetDTO = attr.ib()
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    _conn_pools: AsyncCache[asyncpg.Pool] = attr.ib(default=attr.Factory(AsyncCache))

    _error_transformer = make_async_pg_error_transformer()
    __dialect: Optional[AsyncBIPGDialect] = None

    EXTRA_EXC_CLS: ClassVar[tuple[Type[Exception], ...]] = (
        exc.DataStreamValidationError,
        # I don't have any idea why asyncpg doesn't provide common exception in public interface
        # but there are about 40 different types of exceptions for every case of error
        # I hope private type will work for a long time and we won't put all ~40 exceptions here
        asyncpg.exceptions._base.PostgresError,
        asyncio.TimeoutError,
        # invalid/not available cluster provokes it
        # not connection/timeout error
        OSError,
    )

    @property
    def _dialect(self) -> AsyncBIPGDialect:
        if self.__dialect is not None:
            return self.__dialect
        enforce_collate = self._get_enforce_collate(self._target_dto)
        if enforce_collate is not None:
            self.__dialect = AsyncBIPGDialect(enforce_collate=enforce_collate)
        else:
            self.__dialect = AsyncBIPGDialect()
        return self.__dialect

    def get_conn_line(self, db_name: Optional[str] = None, params: dict[str, Any] = None) -> str:
        params = params or {}
        params["sslrootcert"] = self.get_ssl_cert_path(self._target_dto.ssl_ca)
        return AsyncPGConnLineConstructor(
            dsn_template=self.dsn_template,
            target_dto=self._target_dto,
            dialect_name="postgres",
        ).make_conn_line(db_name=db_name, params=params)

    def get_default_db_name(self) -> Optional[str]:
        return self._target_dto.db_name

    def _convert_date(self, s: str, ignoretz: bool) -> datetime:
        try:
            d = dateutil_parser.parse(s, ignoretz=ignoretz)
        except (dateutil_parser.ParserError, OverflowError) as e:
            LOGGER.info(f"Can't parse date {s} by {ignoretz}")
            # its impossible to get extra info about the position in stream
            # because it's a callback for asyncpg
            raise exc.DataStreamValidationError(value=s) from e
        return d

    async def create_conn_pool(self, db_name_from_query: str) -> asyncpg.Pool:
        db_name = self.get_db_name_for_query(db_name_from_query)
        conn_line = self.get_conn_line(db_name=db_name)
        return await asyncpg.create_pool(conn_line, statement_cache_size=0)

    @asynccontextmanager
    async def _get_connection(self, db_name_from_query: str) -> AsyncIterator[asyncpg.Connection]:
        conn_pool = await self._conn_pools.get(db_name_from_query, generator=self.create_conn_pool)
        async with conn_pool.acquire() as connection:
            # Fix for the "asyncpg.exceptions.InvalidSQLStatementNameError: unnamed prepared statement does not exist"
            # We have already disabled cache by statement_cache_size=0, but it still works,
            # so let's try to use cache inside a transaction.
            async with connection.transaction():
                # There is some decimal-magic in asyncpg
                # and this magic is incompatible with magic in sqlalchemy-psycopg2
                # so lets disable it
                await connection.set_type_codec(
                    "numeric",
                    encoder=str,
                    decoder=lambda x: x,
                    schema="pg_catalog",
                    format="text",
                )
                # we set date-like values to params as strings
                # but asyncpg expects python-objects
                await connection.set_type_codec(
                    "date",
                    encoder=str,
                    decoder=lambda x: self._convert_date(x, ignoretz=True).date(),
                    schema="pg_catalog",
                    format="text",
                )
                await connection.set_type_codec(
                    "timestamp",
                    encoder=str,
                    decoder=lambda x: self._convert_date(x, ignoretz=True),
                    schema="pg_catalog",
                    format="text",
                )
                await connection.set_type_codec(
                    "timestamptz",
                    encoder=str,
                    decoder=lambda x: self._convert_date(x, ignoretz=False),
                    schema="pg_catalog",
                    format="text",
                )
            yield connection

    async def test(self) -> None:
        await self.execute(DBAdapterQuery("select 1"))

    def _make_cursor_info(self, query_attrs: Iterable[asyncpg.Attribute]) -> dict:
        return dict(
            names=[str(a.name) for a in query_attrs],
            driver_types=[self._cursor_type_to_str(a.type.oid) for a in query_attrs],
            db_types=[
                self._cursor_column_to_native_type(
                    (
                        a.name,
                        a.type.oid,
                    ),
                    require=False,
                )
                for a in query_attrs
            ],
            columns=[
                dict(
                    name=str(a.name),
                    postgresql_oid=a.type.oid,
                    postgresql_typname=OID_KNOWLEDGE.get(a.type.oid),  # type: ignore  # TODO: fix
                )
                for a in query_attrs
            ],
            # dashsql convenience:
            postgresql_typnames=[OID_KNOWLEDGE.get(a.type.oid) for a in query_attrs],
        )

    def _get_row_converters(
        self, query_attrs: Iterable[asyncpg.Attribute]
    ) -> tuple[Optional[Callable[[Any], Any]], ...]:
        return tuple(self._convert_bytea if a.type.oid == 17 else None for a in query_attrs)  # `bytea`

    @contextlib.contextmanager
    def execution_context(self) -> typing.Generator[None, None, None]:
        contexts = []

        if self._target_dto.ssl_ca is not None:
            contexts.append(self.ssl_cert_context(self._target_dto.ssl_ca))

        with contextlib.ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            yield

    async def _execute_by_step(self, query: DBAdapterQuery) -> AsyncIterator[ExecutionStep]:
        def make_record(raw_rec: asyncpg.Record, query_attrs: Iterable[asyncpg.Attribute]) -> TBIDataRow:
            row_converters = self._get_row_converters(query_attrs=query_attrs)
            row = tuple(raw_rec.values())
            return tuple(
                (col_converter(val) if col_converter is not None and val is not None else val)
                for val, col_converter in zip(row, row_converters)
            )

        q = query.query
        # exclude ENUM because of
        # https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/lib/sqlalchemy/dialects/postgresql/asyncpg.py#L345
        # and exclude STRINGS because of our BI ENUMS (it's strings)
        # in asyncpg we can skip type annotations for strings, it should work like in psycopg
        compiled_query, params = compile_pg_query(q, self._dialect, exclude_types={DBAPIMock.ENUM, DBAPIMock.STRING})
        debug_query = make_debug_query(compiled_query, params)
        with self.handle_execution_error(debug_query), self.execution_context():
            async with self._get_connection(query.db_name) as conn:
                # prepare works only inside a transaction
                async with conn.transaction():
                    prepared_query = await conn.prepare(compiled_query)
                    cursor_info = self._make_cursor_info(prepared_query.get_attributes())
                    yield ExecutionStepCursorInfo(cursor_info=cursor_info)
                    cursor = await prepared_query.cursor(*params)
                    is_enough = False
                    while not is_enough:
                        result = await cursor.fetch(self._default_chunk_size)
                        if not result:
                            is_enough = True
                        else:
                            chunk = tuple(make_record(record, prepared_query.get_attributes()) for record in result)
                            yield ExecutionStepDataChunk(chunk=chunk)

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        LOGGER.info("Run by async postgres adapter")

        async def _process_chunk(steps: AsyncIterator[ExecutionStep]) -> TBIChunksGen:
            async for step in steps:
                assert isinstance(step, ExecutionStepDataChunk)
                yield step.chunk

        steps = self._execute_by_step(query)
        first_step = await steps.__anext__()
        assert isinstance(first_step, ExecutionStepCursorInfo)
        cursor_info = first_step.cursor_info
        return AsyncRawExecutionResult(
            raw_cursor_info=cursor_info,
            raw_chunk_generator=_process_chunk(steps),
        )

    async def get_schema_names(self, db_ident: DBIdent) -> list[str]:
        result = await self.execute(DBAdapterQuery(PG_LIST_SCHEMA_NAMES))
        schema_names = []
        async for row in result.get_all_rows():
            for value in row:
                schema_names.append(str(value))
        return schema_names

    async def _get_view_names(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        query = sa.text(PG_LIST_VIEW_NAMES).bindparams(
            sa.bindparam(
                "schema",
                schema_ident.schema_name,
                type=sa.types.Unicode,
            )
        )
        result = await self.execute(DBAdapterQuery(query))
        views = []
        async for row in result.get_all_rows():
            views.append(str(row[0]))
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_ident.schema_name,
                table_name=view,
            )
            for view in views
        ]

    async def _get_table_names(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        query = sa.text(PG_LIST_TABLE_NAMES).bindparams(
            sa.bindparam(
                "schema",
                schema_ident.schema_name,
                type=sa.types.Unicode,
            )
        )
        result = await self.execute(DBAdapterQuery(query))
        tables = []
        async for row in result.get_all_rows():
            tables.append(str(row[0]))
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_ident.schema_name,
                table_name=table,
            )
            for table in tables
        ]

    async def _get_tables_single_schema(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        table_list = await self._get_table_names(schema_ident)
        view_list = await self._get_view_names(schema_ident)
        return table_list + view_list

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        if schema_ident.schema_name is not None:
            # For a single schema, plug into the common SA code.
            # (might not be ever used)
            return await self._get_tables_single_schema(schema_ident)

        assert schema_ident.schema_name is None
        db_name = schema_ident.db_name
        result = await self.execute(DBAdapterQuery(PG_LIST_SOURCES_ALL_SCHEMAS_SQL))
        return [
            TableIdent(
                db_name=db_name,
                schema_name=str(schema_name),
                table_name=str(name),
            )
            async for schema_name, name in result.get_all_rows()
        ]

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    # magic copypaste from
    # https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/lib/sqlalchemy/dialects/postgresql/base.py#L3589
    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        if table_ident.schema_name is None:
            result = await self.execute(
                DBAdapterQuery(
                    sa.text(
                        "select relname from pg_class c join pg_namespace n on "
                        "n.oid=c.relnamespace where "
                        "pg_catalog.pg_table_is_visible(c.oid) "
                        "and relname=:name"
                    ).bindparams(
                        sa.bindparam(
                            "name",
                            table_ident.table_name,
                            type_=sa.types.Unicode,
                        )
                    )
                )
            )
        else:
            result = await self.execute(
                DBAdapterQuery(
                    sa.text(
                        "select relname from pg_class c join pg_namespace n on "
                        "n.oid=c.relnamespace where n.nspname=:schema and "
                        "relname=:name"
                    ).bindparams(
                        sa.bindparam(
                            "name",
                            table_ident.table_name,
                            type_=sa.types.Unicode,
                        ),
                        sa.bindparam(
                            "schema",
                            table_ident.schema_name,
                            type_=sa.types.Unicode,
                        ),
                    )
                )
            )
        async for _ in result.get_all_rows():
            return True
        return False

    async def close(self) -> None:
        await self._conn_pools.clear(finalizer=lambda pool: asyncio.wait_for(pool.close(), timeout=2.5))

    @property
    def default_chunk_size(self) -> int:
        return self._default_chunk_size

    @classmethod
    def create(
        cls: Type[_DBA_ASYNC_POSTGRES_TV],
        target_dto: PostgresConnTargetDTO,
        req_ctx_info: DBAdapterScopedRCI,
        default_chunk_size: int,
    ) -> _DBA_ASYNC_POSTGRES_TV:
        return cls(target_dto=target_dto, default_chunk_size=default_chunk_size, req_ctx_info=req_ctx_info)


class AsyncPGConnLineConstructor(ClassicSQLConnLineConstructor[PostgresConnTargetDTO]):
    def _get_dsn_query_params(self) -> dict:
        return {
            "sslmode": "require" if self._target_dto.ssl_enable else "prefer",
        }

    def _get_dsn_params(
        self,
        safe_db_symbols: tuple[str, ...] = (),
        db_name: Optional[str] = None,
        standard_auth: Optional[bool] = True,
    ) -> dict:
        return dict(
            dialect=self._dialect_name,
            user=quote_plus(self._target_dto.username) if standard_auth else None,
            passwd=quote_plus(self._target_dto.password) if standard_auth else None,
            host=quote_plus(self._target_dto.host),
            port=quote_plus(str(self._target_dto.port)),
            db_name=db_name or quote_plus(self._target_dto.db_name or "", safe="".join(safe_db_symbols)),
        )
