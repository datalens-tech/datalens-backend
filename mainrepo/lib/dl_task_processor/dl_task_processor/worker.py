import asyncio
from datetime import timedelta
import socket
import time
from typing import (
    Dict,
    Iterable,
    Optional,
    Union,
)

from arq import Worker as _ArqWorker
from arq.connections import RedisSettings
from arq.worker import async_check_health
import attr

from dl_task_processor.arq_wrapper import (
    EXECUTOR_KEY,
    CronTask,
    arq_base_task,
    create_redis_pool,
)
from dl_task_processor.context import BaseContextFabric
from dl_task_processor.executor import ExecutorFabric

CONTEXT_KEY = "bi_context"


@attr.s
class WorkerSettings:
    # we should not allow forever-fail tasks because it can stop the whole system
    # but (if you really want it) you can provide float('inf')
    retry_hard_limit: Union[int, float] = attr.ib(default=100)
    job_timeout: int = attr.ib(default=600)  # seconds
    health_check_interval: int = attr.ib(default=30)
    health_check_suffix: str = attr.ib(default="bihealthcheck")


@attr.s
class ArqWorker:
    _executor_fab: ExecutorFabric = attr.ib()
    _context_fab: BaseContextFabric = attr.ib()
    _redis_settings: RedisSettings = attr.ib()
    _worker_settings: WorkerSettings = attr.ib()
    _arq_worker: _ArqWorker = attr.ib(default=None)
    _cron_tasks: Iterable[CronTask] = attr.ib(default=[])

    @property
    def health_check_key(self) -> str:
        return f"{socket.gethostname()}::{self._worker_settings.health_check_suffix}"

    async def start(self) -> None:
        redis_pool = await create_redis_pool(self._redis_settings)
        self._arq_worker = _ArqWorker(
            # let's trick strange typing in arq
            # everybody does it o_O
            **{
                "functions": [arq_base_task],
                "on_startup": self.start_executor,
                "on_shutdown": self.stop_executor,
                "max_tries": self._worker_settings.retry_hard_limit,
                "job_timeout": timedelta(seconds=self._worker_settings.job_timeout),
                "retry_jobs": True,
                "health_check_key": self.health_check_key,
                "handle_signals": False,
                "health_check_interval": self._worker_settings.health_check_interval,
                "cron_jobs": self._cron_tasks,
            },
            redis_pool=redis_pool,
        )
        await self._arq_worker.main()

    async def check_health(self) -> bool:
        return bool(not await async_check_health(self._redis_settings, health_check_key=self.health_check_key))

    # param's name is important
    async def start_executor(self, ctx: Dict) -> None:
        context = await self._context_fab.make()
        executor = await self._executor_fab.make(
            context=context,
        )
        ctx[EXECUTOR_KEY] = executor
        ctx[CONTEXT_KEY] = context

    # param's name is important
    async def stop_executor(self, ctx: Dict) -> None:
        assert EXECUTOR_KEY in ctx, "Arq worker has not been run"
        assert CONTEXT_KEY in ctx, "Arq worker has not been run"
        await self._context_fab.tear_down(ctx[CONTEXT_KEY])

    async def stop(self) -> None:
        if self._arq_worker is not None:
            await self._arq_worker.close()


@attr.s
class HealthChecker:
    _worker: ArqWorker = attr.ib()

    async def check(self) -> None:
        # more info is in logs (==stdout)
        try:
            result = await self._worker.check_health()
        except Exception as e:
            raise SystemExit(1) from e
        if result:
            exit(0)
        exit(1)

    async def check_and_raise(self) -> None:
        result = await self._worker.check_health()
        if not result:
            raise ValueError("Health check unsuccessful")

    async def wait_for_ok(self, timeout: Optional[int] = None, cooldown: int = 1) -> None:
        # you should use it only in tests
        start = time.monotonic()
        while (timeout is None) or (time.monotonic() - start < timeout):
            result = await self._worker.check_health()
            if result:
                return
            await asyncio.sleep(cooldown)
        raise RuntimeError(f"Worker did not start in {timeout} seconds")


@attr.s
class ArqWorkerTestWrapper:
    _worker: ArqWorker = attr.ib()
    _loop: asyncio.AbstractEventLoop = attr.ib()
    _task: asyncio.Task = attr.ib(default=None)

    async def start(self) -> ArqWorker:
        self._task = self._loop.create_task(self._worker.start())
        checker = HealthChecker(self._worker)
        await checker.wait_for_ok(20)
        return self._worker

    async def stop(self):
        assert self._task is not None, "Arq worker has not been run"
        try:
            await self._worker.stop()
        except asyncio.exceptions.CancelledError:
            pass
        try:
            await asyncio.gather(self._task)
        except:
            pass
