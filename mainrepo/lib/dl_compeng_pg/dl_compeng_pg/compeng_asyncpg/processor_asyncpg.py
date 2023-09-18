from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Optional

import asyncpg
import attr

from dl_compeng_pg.compeng_asyncpg.exec_adapter_asyncpg import AsyncpgExecAdapter
from dl_compeng_pg.compeng_asyncpg.pool_asyncpg import AsyncpgPoolWrapper
from dl_compeng_pg.compeng_pg_base.processor_base import PostgreSQLOperationProcessor


@attr.s
class AsyncpgOperationProcessor(
    PostgreSQLOperationProcessor[AsyncpgExecAdapter, AsyncpgPoolWrapper, asyncpg.pool.PoolConnectionProxy]
):
    _cmstack: Optional[AsyncExitStack] = attr.ib(init=False, default=None)
    _timeout = 1.5

    async def start(self) -> None:
        assert self._cmstack is None, "should not double-enter"
        cmstack = AsyncExitStack()
        self._cmstack = cmstack
        pg_conn = await cmstack.enter_async_context(self._pg_pool.pool.acquire(timeout=self._timeout))
        self._pg_conn = pg_conn
        await cmstack.enter_async_context(pg_conn.transaction())
        self._pgex_adapter = AsyncpgExecAdapter(conn=pg_conn)

    async def end(self) -> None:
        self._pgex_adapter = None
        await self._cmstack.aclose()  # type: ignore  # TODO: fix
        self._pg_conn = None
        self._cmstack = None
