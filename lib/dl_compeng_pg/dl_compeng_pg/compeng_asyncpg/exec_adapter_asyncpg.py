from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    ClassVar,
    Generator,
    Optional,
    Sequence,
    Union,
)

import asyncpg
import attr
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import pypostgresql

from dl_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from dl_connector_postgresql.core.postgresql_base.error_transformer import make_async_pg_error_transformer
from dl_connector_postgresql.core.postgresql_base.utils import compile_pg_query
from dl_constants.enums import BIType
from dl_core.connectors.base.error_transformer import DbErrorTransformer
from dl_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
from dl_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
)
from dl_sqlalchemy_postgres.asyncpg import DBAPIMock


if TYPE_CHECKING:
    from dl_constants.types import TBIDataValue


LOGGER = logging.getLogger(__name__)


@attr.s
class AsyncpgExecAdapter(PostgreSQLExecAdapterAsync[asyncpg.pool.PoolConnectionProxy]):  # noqa
    # `<PoolConnectionProxy <asyncpg.connection.Connection object at ...> ...>`
    _conn: asyncpg.pool.PoolConnectionProxy = attr.ib()
    _error_transformer: DbErrorTransformer = attr.ib(init=False, factory=make_async_pg_error_transformer)

    _log: ClassVar[logging.Logger] = LOGGER

    @property
    def dialect(self) -> sa.engine.default.DefaultDialect:
        # we should replace it with sqlalchemy.dialects.postgresql.asyncpg
        # now it produces wrong types
        dialect = pypostgresql.dialect(paramstyle="pyformat", dbapi=DBAPIMock())

        dialect.implicit_returning = True
        dialect.supports_native_enum = True
        dialect.supports_smallserial = True  # 9.2+
        dialect._backslash_escapes = False
        dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
        dialect._has_native_hstore = True  # type: ignore  # TODO: fix

        return dialect

    def _compile_query(self, query):  # type: ignore  # TODO: fix
        # exclude {AsyncAdapt_asyncpg_dbapi.ENUM}
        # https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/lib/sqlalchemy/dialects/postgresql/asyncpg.py#L345
        return compile_pg_query(query, self.dialect, exclude_types={DBAPIMock.ENUM}, add_types=False)

    async def _execute(self, query: Union[str, sa.sql.base.Executable]) -> None:
        """Execute query without fetching data"""
        query_text, params = self._compile_query(query)
        await self._conn.execute(query_text, *params)

    async def _execute_ddl(self, query: Union[str, sa.sql.base.Executable]) -> None:
        """Execute a DDL statement"""
        await self._execute(query)

    @contextmanager
    def handle_db_errors(self, query_text: str) -> Generator[None, None, None]:
        try:
            yield
        except Exception as wrapper_exc:
            trans_exc = self._error_transformer.make_bi_error(wrapper_exc=wrapper_exc, debug_compiled_query=query_text)
            raise trans_exc from wrapper_exc

    async def _execute_and_fetch(  # type: ignore  # TODO: fix
        self,
        *,
        query: Union[str, sa.sql.selectable.Select],
        user_types: Sequence[BIType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
        query_id: str,
    ) -> AsyncChunked[list[TBIDataValue]]:
        query_text, params = self._compile_query(query)

        async def chunked_data_gen() -> AsyncGenerator[list[list[TBIDataValue]], None]:
            """Fetch data in chunks"""

            with self.handle_db_errors(query_text=query_text):
                cur = await self._conn.cursor(query_text, *params)
                while True:
                    chunk = []
                    for row in await cur.fetch(chunk_size):
                        assert len(row) == len(user_types), "user_types is not the same length as the data row"
                        chunk.append(
                            [
                                self._tt.cast_for_output(value=value, user_t=user_t)
                                for value, user_t in zip(row, user_types)
                            ]
                        )
                    if not chunk:
                        break
                    yield chunk

        return AsyncChunked(chunked_data=chunked_data_gen())

    async def insert_data_into_table(
        self,
        *,
        table_name: str,
        names: Sequence[str],
        user_types: Sequence[BIType],
        data: AsyncChunkedBase,
    ) -> None:
        """Insert data into a table."""
        self._log.info(f"Inserting data into table {table_name}")

        async for raw_chunk in data.chunks:
            chunk = []
            for row in raw_chunk:
                chunk.append(
                    [self._tt.cast_for_input(value=value, user_t=user_t) for value, user_t in zip(row, user_types)]
                )
            await self._conn.copy_records_to_table(table_name=table_name, columns=names, records=chunk)
