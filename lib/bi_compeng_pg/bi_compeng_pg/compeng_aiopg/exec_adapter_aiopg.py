from __future__ import annotations

import logging
from typing import AsyncGenerator, ClassVar, Optional, Sequence, Union, TYPE_CHECKING

import attr
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.selectable import Select

import aiopg.sa

from bi_constants.enums import BIType

from bi_core.data_processing.streaming import AsyncChunkedBase, AsyncChunked
from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo

from bi_compeng_pg.compeng_pg_base.exec_adapter_base import PostgreSQLExecAdapterAsync

if TYPE_CHECKING:
    from bi_constants.types import TBIDataValue


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
        """ Execute a DDL statement """
        await self._execute(query)

    async def _execute_and_fetch(
            self, *,
            query: Union[Select, str], user_types: Sequence[BIType],
            chunk_size: int, joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
            query_id: str,
    ) -> AsyncChunkedBase[Sequence[TBIDataValue]]:

        async def chunked_data_gen() -> AsyncGenerator[list[list[TBIDataValue]], None]:
            """Fetch data in chunks"""

            result = await self._conn.execute(query)
            while True:
                chunk = []
                for row in await result.fetchmany(chunk_size):
                    assert len(row) == len(user_types), 'user_types is not the same length as the data row'
                    chunk.append([
                        self._tt.cast_for_output(value=value, user_t=user_t)
                        for value, user_t in zip(row.as_tuple(), user_types)
                    ])
                if not chunk:
                    break
                yield chunk

        return AsyncChunked(chunked_data=chunked_data_gen())

    async def insert_data_into_table(
            self, *,
            table_name: str,
            names: Sequence[str],
            user_types: Sequence[BIType],
            data: AsyncChunkedBase,
    ) -> None:
        """Insert data into a table."""

        table = self._make_sa_table(table_name=table_name, names=names, user_types=user_types)
        self._log.info(f'Inserting data into table {table_name}')

        async for raw_chunk in data.chunks:
            chunk = []
            for row in raw_chunk:
                chunk.append([
                    self._tt.cast_for_input(value=value, user_t=user_t)
                    for value, user_t in zip(row, user_types)
                ])
            query = table.insert(values=chunk)
            await self._execute(query=query)
