from __future__ import annotations

import asyncio
from typing import (
    Optional,
    Type,
)

import asyncpg
import attr

from dl_compeng_pg.compeng_pg_base.pool_base import (
    DEFAULT_OPERATION_TIMEOUT,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_SIZE,
    BasePgPoolWrapper,
)


@attr.s
class AsyncpgPoolWrapper(BasePgPoolWrapper):
    _pool: Optional[asyncpg.pool.Pool] = attr.ib()

    @classmethod
    async def connect(
        cls: Type["AsyncpgPoolWrapper"],
        url: str,
        pool_min_size: int = DEFAULT_POOL_MIN_SIZE,  # Initial pool size
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,  # Maximum pool size
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,  # SQL operation timeout
    ) -> "AsyncpgPoolWrapper":
        pool = await asyncpg.create_pool(
            url,
            min_size=pool_min_size,
            max_size=pool_max_size,
            command_timeout=operation_timeout,
            # Should disable the prepared statements and reduce the problems
            # with constants.
            statement_cache_size=0,
        )
        return cls(pool=pool)

    @property
    def pool(self) -> asyncpg.pool.Pool:
        if self._pool is None:
            raise RuntimeError("Asyncpg pool is closed")
        return self._pool

    async def disconnect(self) -> None:
        pool = self._pool
        self._pool = None
        await asyncio.wait_for(pool.close(), timeout=2.5)  # type: ignore  # TODO: fix
