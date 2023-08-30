from __future__ import annotations

import aiopg.sa
import attr

from bi_compeng_pg.compeng_pg_base.processor_base import PostgreSQLOperationProcessor
from bi_compeng_pg.compeng_aiopg.exec_adapter_aiopg import AiopgExecAdapter
from bi_compeng_pg.compeng_aiopg.pool_aiopg import AiopgPoolWrapper


@attr.s
class AiopgOperationProcessor(
        PostgreSQLOperationProcessor[
            AiopgExecAdapter,
            AiopgPoolWrapper,
            aiopg.sa.SAConnection
        ]
):

    async def start(self) -> None:
        self._pg_conn = await self._pg_pool.pool.acquire()
        self._pgex_adapter = AiopgExecAdapter(conn=self._pg_conn)  # type: ignore  # TODO: fix

    async def end(self) -> None:
        self._pgex_adapter = None
        if self._pg_conn is not None:
            await self._pg_conn.close()
        self._pg_conn = None
