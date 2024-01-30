from __future__ import annotations

import aiopg.sa
import attr

from dl_compeng_pg.compeng_aiopg.exec_adapter_aiopg import AiopgExecAdapter
from dl_compeng_pg.compeng_aiopg.pool_aiopg import AiopgPoolWrapper
from dl_compeng_pg.compeng_pg_base.processor_base import PostgreSQLOperationProcessor


@attr.s
class AiopgOperationProcessor(PostgreSQLOperationProcessor[AiopgPoolWrapper, aiopg.sa.SAConnection]):
    async def start(self) -> None:
        self._pg_conn = await self._pg_pool.pool.acquire()
        self._db_ex_adapter = AiopgExecAdapter(
            service_registry=self.service_registry,
            reporting_enabled=self._reporting_enabled,
            conn=self._pg_conn,  # type: ignore  # 2024-01-29 # TODO: Argument "conn" to "AiopgExecAdapter" has incompatible type "SAConnection | None"; expected "SAConnection"  [arg-type]
            cache_options_builder=self._cache_options_builder,
        )  # type: ignore  # TODO: fix

    async def end(self) -> None:
        self._db_ex_adapter = None
        if self._pg_conn is not None:
            await self._pg_conn.close()
        self._pg_conn = None
