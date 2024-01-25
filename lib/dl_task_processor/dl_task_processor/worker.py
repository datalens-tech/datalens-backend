import asyncio
from datetime import (
    datetime,
    timedelta,
)
import logging
import socket
import time
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    Sequence,
)

from arq.connections import RedisSettings
from arq.typing import SecondsTimedelta
from arq.utils import to_seconds
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
from dl_task_processor.upstream_worker import Worker


LOGGER = logging.getLogger(__name__)

CONTEXT_KEY = "bi_context"


class WorkerMetricsSenderProtocol(Protocol):
    async def send(self, timestamp: float, metrics: dict[str, Any]) -> None:
        ...


class DLArqWorker(Worker):
    def __init__(
        self,
        health_check_record_ttl: SecondsTimedelta,
        metrics_sender: Optional[WorkerMetricsSenderProtocol] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._metrics_sender = metrics_sender
        self.health_check_record_ttl = to_seconds(health_check_record_ttl)

    async def record_health(self) -> None:
        """Copy and paste from the base class, but using health_check_record_ttl & sending metrics"""

        now_ts = time.time()
        if (now_ts - self._last_health_check) < self.health_check_interval:
            return
        self._last_health_check = now_ts
        pending_tasks = sum(not t.done() for t in self.tasks.values())
        queued = await self.pool.zcard(self.queue_name)
        info = (
            f"{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} j_failed={self.jobs_failed} "
            f"j_retried={self.jobs_retried} j_ongoing={pending_tasks} queued={queued}"
        )
        await self.pool.psetex(  # type: ignore[no-untyped-call]
            self.health_check_key, int(self.health_check_record_ttl * 1000), info.encode()
        )
        log_suffix = info[info.index("j_complete=") :]
        if self._last_health_check_log and log_suffix != self._last_health_check_log:  # TODO?: log health always?
            LOGGER.info("recording health: %s", info)
            self._last_health_check_log = log_suffix
        elif not self._last_health_check_log:
            self._last_health_check_log = log_suffix

        if self._metrics_sender is not None:
            await self._metrics_sender.send(
                timestamp=now_ts,
                metrics=dict(
                    j_complete=self.jobs_complete,
                    j_failed=self.jobs_failed,
                    j_retried=self.jobs_retried,
                    j_ongoing=pending_tasks,
                    queued=queued,
                ),
            )


@attr.s
class WorkerSettings:
    # we should not allow forever-fail tasks because it can stop the whole system
    # but (if you really want it) you can provide float('inf')
    retry_hard_limit: int = attr.ib(default=100)
    job_timeout: int = attr.ib(default=600)  # seconds
    health_check_interval: int = attr.ib(default=30)  # how often the HC record is set
    health_check_record_ttl: int = attr.ib(  # ttl should generally be greater than the HC interval with a margin
        default=attr.Factory(
            lambda self: self.health_check_interval * 3,  # just to be safe
            takes_self=True,
        ),
    )
    health_check_suffix: str = attr.ib(default="bihealthcheck")


@attr.s
class ArqWorker:
    _executor_fab: ExecutorFabric = attr.ib()
    _context_fab: BaseContextFabric = attr.ib()
    _redis_settings: RedisSettings = attr.ib()
    _worker_settings: WorkerSettings = attr.ib()
    _metrics_sender: Optional[WorkerMetricsSenderProtocol] = attr.ib(default=None)
    _arq_worker: DLArqWorker = attr.ib(default=None)
    _cron_tasks: Sequence[CronTask] = attr.ib(factory=list)

    @property
    def health_check_key(self) -> str:
        return f"{socket.gethostname()}::{self._worker_settings.health_check_suffix}"

    async def start(self) -> None:
        redis_pool = await create_redis_pool(self._redis_settings)
        self._arq_worker = DLArqWorker(
            functions=[arq_base_task],
            on_startup=self.start_executor,
            on_shutdown=self.stop_executor,
            max_tries=self._worker_settings.retry_hard_limit,
            job_timeout=timedelta(seconds=self._worker_settings.job_timeout),
            retry_jobs=True,
            health_check_key=self.health_check_key,
            handle_signals=False,
            health_check_interval=self._worker_settings.health_check_interval,
            health_check_record_ttl=self._worker_settings.health_check_record_ttl,
            cron_jobs=self._cron_tasks,
            redis_pool=redis_pool,
            metrics_sender=self._metrics_sender,
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

    async def is_ok(self) -> bool:
        return await self._worker.check_health()

    async def check(self) -> None:
        # more info is in logs (==stdout)
        try:
            result = await self.is_ok()
        except Exception as e:
            raise SystemExit(1) from e
        if result:
            exit(0)
        exit(1)

    async def check_and_raise(self) -> None:
        result = await self.is_ok()
        if not result:
            raise ValueError("Health check unsuccessful")

    async def wait_for_ok(self, timeout: Optional[int] = None, cooldown: int = 1) -> None:
        # you should use it only in tests
        start = time.monotonic()
        while (timeout is None) or (time.monotonic() - start < timeout):
            result = await self.is_ok()
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

    async def stop(self) -> None:
        assert self._task is not None, "Arq worker has not been run"
        try:
            await self._worker.stop()
        except asyncio.exceptions.CancelledError:
            pass
        try:
            await asyncio.gather(self._task)
        except:
            pass
