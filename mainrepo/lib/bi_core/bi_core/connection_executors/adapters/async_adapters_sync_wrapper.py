from __future__ import annotations

import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Optional,
    Set,
)
import weakref

import attr

from bi_api_commons.aio.async_wrapper_for_sync_generator import (
    EndOfStream,
    Job,
)
from bi_core.connection_executors.adapters.adapters_base import SyncDirectDBAdapter
from bi_core.connection_executors.adapters.async_adapters_base import (
    AsyncDBAdapter,
    AsyncRawExecutionResult,
)
from bi_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
    ExecutionStepCursorInfo,
    ExecutionStepDataChunk,
    RawSchemaInfo,
)
from bi_utils.aio import ContextVarExecutor

if TYPE_CHECKING:
    from bi_constants.types import TBIChunksGen
    from bi_core.connection_models.common_models import (
        DBIdent,
        SchemaIdent,
        TableDefinition,
        TableIdent,
    )


LOGGER = logging.getLogger(__name__)


@attr.s()
class AsyncWrapperForSyncAdapter(AsyncDBAdapter):
    _sync_adapter: SyncDirectDBAdapter = attr.ib()
    _tpe: ContextVarExecutor = attr.ib()
    _loop: asyncio.AbstractEventLoop = attr.ib(init=False, factory=asyncio.get_running_loop)
    _gen_wrappers_weak_set: Set[Job] = attr.ib(init=False, factory=weakref.WeakSet)

    @attr.s(cmp=False, hash=False)
    class AdapterExecuteJob(Job[ExecutionStep]):  # noqa
        """Class to be instantiated on connection executor"""

        _adapter: SyncDirectDBAdapter = attr.ib(kw_only=True)
        _query: DBAdapterQuery = attr.ib(kw_only=True)

        def make_generator(self) -> Generator[ExecutionStep, None, None]:
            return self._adapter.execute_by_steps(self._query)

    async def execute(self, query: DBAdapterQuery) -> AsyncRawExecutionResult:
        # TODO FIX: Separate TPE
        gen_wrapper = self.AdapterExecuteJob(
            service_tpe=self._tpe,
            workers_tpe=self._tpe,
            query=query,
            adapter=self._sync_adapter,
        )
        self._gen_wrappers_weak_set.add(gen_wrapper)
        await gen_wrapper.run()

        first_msg = await gen_wrapper.get_next()
        if not isinstance(first_msg, ExecutionStepCursorInfo):
            # TODO FIX: Custom exception
            raise ValueError(f"Unexpected type of first execution message in queue: {type(first_msg)}")

        async def data_generator() -> TBIChunksGen:
            try:
                while True:
                    msg = await gen_wrapper.get_next()

                    if isinstance(msg, ExecutionStepDataChunk):
                        yield msg.chunk
                    elif msg is None:
                        break
                    else:
                        # TODO FIX: Custom exception
                        raise ValueError(f"Unexpected type of execution message in queue: {type(msg)}")
            except EndOfStream:
                return
            finally:
                await gen_wrapper.cancel()

        return AsyncRawExecutionResult(
            raw_cursor_info=first_msg.cursor_info,
            raw_chunk_generator=data_generator(),
        )

    async def test(self) -> None:
        await self._loop.run_in_executor(self._tpe, self._sync_adapter.test)

    async def get_db_version(self, db_ident: DBIdent) -> Optional[str]:
        return await self._loop.run_in_executor(self._tpe, self._sync_adapter.get_db_version, db_ident)

    async def get_schema_names(self, db_ident: DBIdent) -> List[str]:
        return await self._loop.run_in_executor(self._tpe, self._sync_adapter.get_schema_names, db_ident)

    async def get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        return await self._loop.run_in_executor(self._tpe, self._sync_adapter.get_tables, schema_ident)

    async def get_table_info(self, table_def: TableDefinition, fetch_idx_info: bool) -> RawSchemaInfo:
        return await self._loop.run_in_executor(
            self._tpe,
            self._sync_adapter.get_table_info,
            table_def,
            fetch_idx_info,
        )

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        return await self._loop.run_in_executor(self._tpe, self._sync_adapter.is_table_exists, table_ident)

    async def close(self) -> None:
        LOGGER.info("Starting async executor wrapper close procedure")
        fut_list = []
        for wr in self._gen_wrappers_weak_set:
            fut_list.append(wr.cancel())
        if fut_list:
            LOGGER.info("Waiting for %s running generators to be closed", len(fut_list))
            await asyncio.gather(*fut_list, return_exceptions=True)
        else:
            LOGGER.info("No running generator found")
        # TODO FIX: add timeout
        LOGGER.info("Waiting for sync adapter to be closed")
        await self._loop.run_in_executor(self._tpe, self._sync_adapter.close)
        LOGGER.info("Sync adapter was closed")
