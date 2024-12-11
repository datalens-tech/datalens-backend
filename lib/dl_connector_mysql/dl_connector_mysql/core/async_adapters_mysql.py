from __future__ import annotations

import asyncio
import contextlib
from functools import partial
import logging
import os
import ssl
from typing import (
    Any,
    AsyncIterator,
    ContextManager,
    Generator,
    Optional,
    Type,
    TypeVar,
)

import aiomysql.sa
import attr
from pymysql.err import (
    OperationalError,
    ProgrammingError,
)
import sqlalchemy as sa

from dl_app_tools.profiling_base import generic_profiler_async
from dl_configs.utils import get_root_certificates_path
from dl_constants.types import TBIChunksGen
from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncDBVersionAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.db_version import AsyncDBVersionAdapterActionViaFunctionQuery
from dl_core.connection_executors.adapters.async_adapters_base import (
    AsyncCache,
    AsyncDirectDBAdapter,
    AsyncRawExecutionResult,
)
from dl_core.connection_executors.adapters.mixins import (
    WithDatabaseNameOverride,
    WithMinimalCursorInfo,
    WithNoneRowConverters,
)
from dl_core.connection_executors.adapters.sa_utils import compile_query_for_debug
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawSchemaInfo,
)
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_models import (
    SchemaIdent,
    TableDefinition,
    TableIdent,
)
from dl_core.connectors.base.error_handling import ETBasedExceptionMaker
from dl_sqlalchemy_mysql.base import DLMYSQLDialect

from dl_connector_mysql.core.adapters_base_mysql import BaseMySQLAdapter
from dl_connector_mysql.core.error_transformer import async_mysql_db_error_transformer
from dl_connector_mysql.core.target_dto import MySQLConnTargetDTO
from dl_connector_mysql.core.utils import compile_mysql_query


LOGGER = logging.getLogger(__name__)

_DBA_ASYNC_MYSQL_TV = TypeVar("_DBA_ASYNC_MYSQL_TV", bound="AsyncMySQLAdapter")

MYSQL_USE_TLS = int(os.environ.get("MYSQL_USE_TLS", 0))


@attr.s(cmp=False, kw_only=True)
class AsyncMySQLAdapter(
    WithDatabaseNameOverride,
    WithNoneRowConverters,
    ETBasedExceptionMaker,
    BaseMySQLAdapter,
    AsyncDirectDBAdapter,
    WithMinimalCursorInfo,
):
    _target_dto: MySQLConnTargetDTO = attr.ib()
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    _default_chunk_size: int = attr.ib()

    _engines: AsyncCache[aiomysql.sa.Engine] = attr.ib(default=attr.Factory(AsyncCache))

    _error_transformer = async_mysql_db_error_transformer

    EXTRA_EXC_CLS = (
        OperationalError,
        ProgrammingError,
    )

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        return AsyncDBVersionAdapterActionViaFunctionQuery(async_adapter=self)

    @property
    def _dialect(self) -> sa.engine.default.DefaultDialect:
        dialect = DLMYSQLDialect(paramstyle="pyformat")
        return dialect

    def _cursor_column_to_nullable(self, cursor_col: tuple[Any, ...]) -> Optional[bool]:
        # See https://aiomysql.readthedocs.io/en/latest/cursors.html#Cursor.description
        # Although there are no known `nullable=False` cases for subselects in MySQL,
        # let's use here `null_ok` field from cursor rather than hardcoded value
        return cursor_col[6]

    @classmethod
    def create(
        cls: Type[_DBA_ASYNC_MYSQL_TV],
        target_dto: MySQLConnTargetDTO,
        req_ctx_info: DBAdapterScopedRCI,
        default_chunk_size: int,
    ) -> _DBA_ASYNC_MYSQL_TV:
        return cls(target_dto=target_dto, req_ctx_info=req_ctx_info, default_chunk_size=default_chunk_size)

    def get_default_db_name(self) -> Optional[str]:
        return self._target_dto.db_name

    def get_target_host(self) -> Optional[str]:
        return self._target_dto.host

    # TODO: get rid of use_ssl_backwards_compatibility after migration to TLS
    async def _create_engine(
        self, db_name: str, use_ssl_backwards_compatibility: Optional[bool] = None
    ) -> aiomysql.sa.Engine:
        if use_ssl_backwards_compatibility is not None:
            ssl_ctx = (
                ssl.create_default_context(cafile=get_root_certificates_path())
                if use_ssl_backwards_compatibility
                else None
            )
        else:
            ssl_ctx = (
                ssl.create_default_context(
                    cafile=self.get_ssl_cert_path(self._target_dto.ssl_ca)
                    if self._target_dto.ssl_ca
                    else get_root_certificates_path()
                )
                if self._target_dto.ssl_enable
                else None
            )

        return await aiomysql.sa.create_engine(
            host=self._target_dto.host,
            port=self._target_dto.port,
            user=self._target_dto.username,
            password=self._target_dto.password,
            db=db_name,
            dialect=self._dialect,
            ssl=ssl_ctx,
        )

    # TODO: get rid of _get_engine after migration to TLS
    async def _get_engine(self, db_name: str) -> aiomysql.sa.Engine:
        try:
            create_engine_using_no_ssl = partial(self._create_engine, use_ssl_backwards_compatibility=False)
            return await self._engines.get(db_name, generator=create_engine_using_no_ssl)
        except OperationalError as err:
            # 3159 = Connections using insecure transport are prohibited while --require_secure_transport=ON.
            # This means we have to use SSL
            if err.args[0] == 3159:
                LOGGER.info("Using SSL for async MySQL connection")
                create_engine_using_ssl = partial(self._create_engine, use_ssl_backwards_compatibility=True)
                return await self._engines.get(db_name, generator=create_engine_using_ssl)
            else:
                raise

    @contextlib.contextmanager
    def execution_context(self) -> Generator[None, None, None]:
        contexts: list[ContextManager[None]] = []

        if self._target_dto.ssl_ca is not None:
            contexts.append(self.ssl_cert_context(self._target_dto.ssl_ca))

        with contextlib.ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            try:
                yield
            finally:
                stack.close()

    # TODO: get rid of MYSQL_USE_TLS after migration to TLS
    @contextlib.asynccontextmanager
    async def _get_connection(self, db_name_from_query: Optional[str]) -> AsyncIterator[aiomysql.sa.SAConnection]:
        db_name = self.get_db_name_for_query(db_name_from_query)

        if not MYSQL_USE_TLS:
            engine = await self._get_engine(db_name)
        else:
            engine = await self._engines.get(db_name, generator=self._create_engine)

        async with engine.acquire() as connection:
            yield connection

    async def _execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> AsyncIterator[ExecutionStep]:
        """Generator that yielding messages with data chunks and execution meta-info"""

        chunk_size = db_adapter_query.get_effective_chunk_size(self._default_chunk_size)
        query = db_adapter_query.query
        debug_compiled_query = db_adapter_query.debug_compiled_query
        escape_percent = not db_adapter_query.is_dashsql_query  # DON'T escape only for dashsql
        compiled_query, compiled_query_parameters = compile_mysql_query(
            query, dialect=self._dialect, escape_percent=escape_percent
        )
        debug_query = None
        if self._target_dto.pass_db_query_to_user:
            if debug_compiled_query is not None:
                debug_query = debug_compiled_query
            else:
                debug_query = query if isinstance(query, str) else compile_query_for_debug(query, self._dialect)

        with self.handle_execution_error(debug_query), self.execution_context():
            async with self._get_connection(db_adapter_query.db_name) as conn:
                result = await conn.execute(compiled_query, compiled_query_parameters)
                cursor_info = ExecutionStepCursorInfo(
                    cursor_info=self._make_cursor_info(result.cursor),
                    raw_cursor_description=list(result.cursor.description),
                )
                yield cursor_info

                row_converters = self._get_row_converters(cursor_info=cursor_info)
                while True:
                    LOGGER.info("Fetching %s rows (conn %s)", chunk_size, conn)
                    rows = await result.fetchmany(chunk_size)
                    if not rows:
                        LOGGER.info("No rows remaining")
                        break

                    LOGGER.info("Rows fetched, yielding")
                    yield ExecutionStepDataChunk(
                        tuple(
                            tuple(
                                (col_converter(val) if col_converter is not None and val is not None else val)
                                for val, col_converter in zip(
                                    [row[col_name] for col_name in cursor_info.cursor_info["names"]],
                                    row_converters,
                                    strict=True,
                                )
                            )
                            for row in rows
                        )
                    )

    @generic_profiler_async("db-full")  # type: ignore  # TODO: fix
    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        steps = self._execute_by_steps(query)
        cursor_info_step = await steps.__anext__()

        if not isinstance(cursor_info_step, ExecutionStepCursorInfo):
            raise RuntimeError(f"Unexpected type of first step from database: {cursor_info_step}")

        async def _process_chunk(steps: AsyncIterator[ExecutionStep]) -> TBIChunksGen:
            async for step in steps:
                if not isinstance(step, ExecutionStepDataChunk):
                    raise RuntimeError(f"Unexpected type of non-first step from database: {step}")
                yield step.chunk

        return AsyncRawExecutionResult(
            raw_cursor_info=cursor_info_step.cursor_info,
            raw_chunk_generator=_process_chunk(steps),
        )

    async def close(self) -> None:
        async def _finalizer(engine: aiomysql.sa.Engine) -> None:
            engine.close()
            await asyncio.wait_for(engine.wait_closed(), timeout=2.5)

        await self._engines.clear(finalizer=_finalizer)

    async def test(self) -> None:
        await self.execute(DBAdapterQuery("SELECT 1"))

    async def get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        raise NotImplementedError()

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        raise NotImplementedError()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        raise NotImplementedError()
