from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Optional,
    Sequence,
    Union,
)

import aiopg.sa
import attr
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.selectable import Select

from dl_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync
from dl_constants.enums import UserDataType
from dl_core.data_processing.cache.primitives import LocalKeyRepresentation
from dl_core.data_processing.prepared_components.primitives import PreparedFromInfo
from dl_core.data_processing.processing.context import OpExecutionContext
from dl_core.data_processing.streaming import (
    AsyncChunked,
    AsyncChunkedBase,
)


if TYPE_CHECKING:
    from dl_constants.types import TBIDataValue


DEFAULT_CHUNK_SIZE = 1000

LOGGER = logging.getLogger(__name__)


@attr.s
class AiopgExecAdapter(PostgreSQLExecAdapterAsync[aiopg.sa.SAConnection]):  # noqa
    _conn: aiopg.sa.SAConnection = attr.ib()
    _log: ClassVar[logging.Logger] = LOGGER

    async def _execute(self, query: Union[str, Executable]) -> None:
        """Execute query without fetching data"""
        await self._conn.execute(query)

    async def _execute_ddl(self, query: Union[str, Executable]) -> None:
        """Execute a DDL statement"""
        await self._execute(query)

    async def _execute_and_fetch(
        self,
        *,
        query: Select | str,
        user_types: Sequence[UserDataType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedFromInfo] = None,
        query_id: str,
        ctx: OpExecutionContext,
        data_key: LocalKeyRepresentation,
        preparation_callback: Optional[Callable[[], Awaitable[None]]],
    ) -> AsyncChunkedBase[Sequence[TBIDataValue]]:
        if preparation_callback is not None:
            await preparation_callback()

        async def chunked_data_gen() -> AsyncGenerator[list[list[TBIDataValue]], None]:
            """Fetch data in chunks"""

            result = await self._conn.execute(query)
            while True:
                chunk = []
                for row in await result.fetchmany(chunk_size):
                    assert len(row) == len(user_types), "user_types is not the same length as the data row"
                    chunk.append(
                        [
                            self._tt.cast_for_output(value=value, user_t=user_t)
                            for value, user_t in zip(row.as_tuple(), user_types, strict=True)
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
        user_types: Sequence[UserDataType],
        data: AsyncChunkedBase,
    ) -> None:
        """Insert data into a table."""

        table = self._make_sa_table(table_name=table_name, names=names, user_types=user_types)
        self._log.info(f"Inserting data into table {table_name}")

        async for raw_chunk in data.chunks:
            chunk = []
            for row in raw_chunk:
                chunk.append(
                    [
                        self._tt.cast_for_input(value=value, user_t=user_t)
                        for value, user_t in zip(row, user_types, strict=True)
                    ]
                )
            query = table.insert(values=chunk)
            await self._execute(query=query)
