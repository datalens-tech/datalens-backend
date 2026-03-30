import asyncio
from collections.abc import AsyncIterator
import contextlib
import logging
from typing import (
    Any,
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
from dl_core.connection_executors.adapters.sa_utils import (
    compile_query_for_debug,
    compile_query_for_inspector,
)
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
)
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connectors.base.error_handling import ETBasedExceptionMaker
from dl_sqlalchemy_starrocks.base import BIStarRocksDialect

from dl_connector_mysql.core.utils import compile_mysql_query

# StarRocks is MySQL-compatible, so we can use MySQL query compilation
from dl_connector_starrocks.core.adapters_base_starrocks import BaseStarRocksAdapter
from dl_connector_starrocks.core.error_transformer import async_starrocks_db_error_transformer
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


LOGGER = logging.getLogger(__name__)

_DBA_ASYNC_STARROCKS_TV = TypeVar("_DBA_ASYNC_STARROCKS_TV", bound="AsyncStarRocksAdapter")


@attr.s(cmp=False, kw_only=True)
class AsyncStarRocksAdapter(
    WithDatabaseNameOverride,
    WithNoneRowConverters,
    ETBasedExceptionMaker,
    BaseStarRocksAdapter,
    AsyncDirectDBAdapter,
    WithMinimalCursorInfo,
):
    _req_ctx_info: DBAdapterScopedRCI = attr.ib()
    _default_chunk_size: int = attr.ib()

    _engines: AsyncCache[aiomysql.sa.Engine] = attr.ib(default=attr.Factory(AsyncCache))

    _error_transformer = async_starrocks_db_error_transformer

    EXTRA_EXC_CLS = (
        OperationalError,
        ProgrammingError,
        RuntimeError,
    )

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        return AsyncDBVersionAdapterActionViaFunctionQuery(async_adapter=self)

    @property
    def _dialect(self) -> sa.engine.default.DefaultDialect:
        dialect = BIStarRocksDialect(paramstyle="pyformat")
        return dialect

    def _cursor_column_to_nullable(self, cursor_col: tuple[Any, ...]) -> bool | None:
        # See https://aiomysql.readthedocs.io/en/latest/cursors.html#Cursor.description
        # StarRocks uses MySQL protocol, so cursor description is the same
        return cursor_col[6]

    @classmethod
    def create(
        cls: type[_DBA_ASYNC_STARROCKS_TV],
        target_dto: StarRocksConnTargetDTO,
        req_ctx_info: DBAdapterScopedRCI,
        default_chunk_size: int,
    ) -> _DBA_ASYNC_STARROCKS_TV:
        return cls(target_dto=target_dto, req_ctx_info=req_ctx_info, default_chunk_size=default_chunk_size)

    def get_default_db_name(self) -> str | None:
        return ""  # StarRocks doesn't require a catalog to connect

    def get_target_host(self) -> str | None:
        return self._target_dto.host

    def _get_ssl_ctx(self, force_ssl: bool = False) -> dict | None:
        # TODO: Add SSL support for StarRocks: BI-7169
        return None

    async def _create_engine(
        self,
        db_name: str,
        force_ssl: bool = False,
    ) -> aiomysql.sa.Engine:
        return await aiomysql.sa.create_engine(
            host=self._target_dto.host,
            port=self._target_dto.port,
            user=self._target_dto.username,
            password=self._target_dto.password,
            db="",
            dialect=self._dialect,
            ssl=self._get_ssl_ctx(force_ssl),
            charset="utf8mb4",
            local_infile=0,
        )

    async def _get_engine(self, db_name: str) -> aiomysql.sa.Engine:
        return await self._engines.get("", generator=self._create_engine)

    @contextlib.asynccontextmanager
    async def _get_connection(self, db_name_from_query: str | None) -> AsyncIterator[aiomysql.sa.SAConnection]:
        db_name = self.get_db_name_for_query(db_name_from_query)
        engine = await self._get_engine(db_name)

        async with engine.acquire() as connection:
            yield connection

    async def _execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> AsyncIterator[ExecutionStep]:
        chunk_size = db_adapter_query.get_effective_chunk_size(self._default_chunk_size)
        query = db_adapter_query.query
        escape_percent = not db_adapter_query.is_dashsql_query
        compiled_query, compiled_query_parameters = compile_mysql_query(
            query, dialect=self._dialect, escape_percent=escape_percent
        )

        debug_query = None
        inspector_query = None
        if self._target_dto.pass_db_query_to_user:
            inspector_query = db_adapter_query.inspector_query or compile_query_for_inspector(
                query, self._dialect, obfuscation_engine=self._req_ctx_info.obfuscation_engine
            )
            debug_query = db_adapter_query.debug_compiled_query or compile_query_for_debug(query, self._dialect)

        with self.handle_execution_error(debug_query=debug_query, inspector_query=inspector_query):
            async with self._get_connection(db_adapter_query.db_name) as conn:
                result = await conn.execute(compiled_query, compiled_query_parameters)
                if result.cursor.description is None:
                    cursor_info = ExecutionStepCursorInfo(
                        cursor_info={"names": [], "driver_types": [], "db_types": [], "columns": []},
                        raw_cursor_description=[],
                    )
                else:
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
