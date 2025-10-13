import asyncio
import contextlib
import logging
from typing import AsyncGenerator

import temporalio.worker

import dl_utils


@contextlib.asynccontextmanager
async def worker_run_context(
    worker: temporalio.worker.Worker,
) -> AsyncGenerator[temporalio.worker.Worker, None]:
    run_task = asyncio.create_task(worker.run())

    async def _is_worker_running() -> bool:
        return worker.is_running

    await dl_utils.await_for(
        name="temporal worker to start",
        condition=_is_worker_running,
        timeout=30,
        interval=1,
        log_func=logging.info,
    )

    try:
        yield worker
    finally:
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
