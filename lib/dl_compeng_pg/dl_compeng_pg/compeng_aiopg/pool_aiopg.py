from __future__ import annotations

from typing import Optional

import aiopg.sa
import attr

from dl_compeng_pg.compeng_pg_base.pool_base import (
    DEFAULT_OPERATION_TIMEOUT,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_SIZE,
    BasePgPoolWrapper,
)


@attr.s
class AiopgPoolWrapper(BasePgPoolWrapper):
    _pool: Optional[aiopg.sa.Engine] = attr.ib()

    @classmethod
    async def connect(
        cls: type["AiopgPoolWrapper"],
        url: str,
        pool_min_size: int = DEFAULT_POOL_MIN_SIZE,  # Initial pool size
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,  # Maximum pool size
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,  # SQL operation timeout
    ) -> "AiopgPoolWrapper":
        pool = await aiopg.sa.create_engine(
            url,
            minsize=pool_min_size,
            maxsize=pool_max_size,
            timeout=operation_timeout,
        )
        return cls(pool=pool)

    @property
    def pool(self) -> aiopg.sa.Engine:
        if self._pool is None:
            raise RuntimeError("Aiopg pool is closed")
        return self._pool

    async def disconnect(self) -> None:
        if self._pool is None:
            raise RuntimeError("Aiopg pool is closed")

        pool = self._pool
        self._pool = None
        pool.terminate()
        await pool.wait_closed()
