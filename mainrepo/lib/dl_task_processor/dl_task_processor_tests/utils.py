import asyncio
import logging
from typing import Optional

import arq
import attr

from dl_configs.settings_submodels import RedisSettings
from dl_task_processor.arq_wrapper import create_redis_pool
from dl_task_processor.context import (
    BaseContext,
    BaseContextFabric,
)
from dl_task_processor.executor import Executor
from dl_task_processor.processor import (
    LocalProcessorImpl,
    TaskProcessor,
    make_task_processor,
)
from dl_task_processor.state import (
    DummyStateImpl,
    TaskState,
)
from dl_task_processor.task import (
    BaseExecutorTask,
    BaseTaskMeta,
    Retry,
    Success,
    TaskName,
    TaskRegistry,
    TaskResult,
)
from dl_utils.aio import ContextVarExecutor

LOGGER = logging.getLogger(__name__)

BROKEN_MARK = "mark_broken_task"


@attr.s
class Context(BaseContext):
    tpe: ContextVarExecutor = attr.ib()
    tp: TaskProcessor = attr.ib()
    _redis_pool: Optional[arq.ArqRedis] = attr.ib(default=None)


@attr.s
class DummyContext(BaseContext):
    tpe: ContextVarExecutor = attr.ib()


class LocalContextFab(BaseContextFabric):
    async def make(self) -> Context:
        state = TaskState(DummyStateImpl())
        # yep, we create another executor
        executor = Executor(context=DummyContext(tpe=ContextVarExecutor()), state=state, registry=REGISTRY)
        impl = LocalProcessorImpl(executor)
        processor = TaskProcessor(impl=impl, state=state)
        return Context(
            tpe=ContextVarExecutor(),
            tp=processor,
        )

    async def tear_down(self, inst: Context):
        inst.tpe.shutdown()


@attr.s
class ARQContextFab(BaseContextFabric):
    _redis_settings: RedisSettings = attr.ib()

    async def make(self) -> Context:
        redis_pool = await create_redis_pool(self._redis_settings)
        return Context(
            tpe=ContextVarExecutor(),
            tp=make_task_processor(
                redis_pool=redis_pool,
                request_id=None,
            ),
            redis_pool=redis_pool,
        )

    async def tear_down(self, inst: Context):
        inst.tpe.shutdown()
        if inst._redis_pool is not None:
            await inst._redis_pool.close()


@attr.s
class SomeTaskInterface(BaseTaskMeta):
    name = TaskName("some_task")
    foo: str = attr.ib()


@attr.s
class BrokenTaskInterface(BaseTaskMeta):
    name = TaskName("broken_task")
    bar: str = attr.ib()


@attr.s
class RetryTaskInterface(BaseTaskMeta):
    name = TaskName("retry_task")
    foobar: str = attr.ib()


@attr.s
class TestIdsTaskInterface(BaseTaskMeta):
    name = TaskName("test_ids_task")
    expected_request_id: str = attr.ib()


@attr.s
class ScheduleFromTaskTaskInterface(BaseTaskMeta):
    name = TaskName("test_schedule_from_task_task")


@attr.s
class SomeTask(BaseExecutorTask[SomeTaskInterface, Context]):
    cls_meta = SomeTaskInterface

    async def run(self) -> TaskResult:
        LOGGER.info(f"Its some task with a {self._ctx} {self.meta.foo}")
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                self._ctx.tpe,
                lambda t: ", ".join((str(i) for i in range(t))),
                10,
            )
        except Exception as ex:
            LOGGER.exception(ex)
            raise
        LOGGER.info(result)
        return Success()


def some_sync_function_with_logs() -> None:
    LOGGER.info(f"{BROKEN_MARK}")


@attr.s
class BrokenTask(BaseExecutorTask[BrokenTaskInterface, Context]):
    cls_meta = BrokenTaskInterface

    async def run(self) -> TaskResult:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(self._ctx.tpe, some_sync_function_with_logs)
        # let's hack checkers
        if int(1) != 1:
            return Success()
        raise Exception()


@attr.s
class TestIdsTask(BaseExecutorTask[TestIdsTaskInterface, Context]):
    cls_meta = TestIdsTaskInterface

    async def run(self) -> TaskResult:
        assert self._request_id == self.meta.expected_request_id
        assert self._instance_id
        assert self._run_id
        return Success()


@attr.s
class RetryTask(BaseExecutorTask[RetryTaskInterface, Context]):
    cls_meta = RetryTaskInterface

    async def run(self) -> TaskResult:
        LOGGER.info(f"Its retry task with a {self._ctx} {self.meta.foobar}")
        return Retry(
            attempts=2,
            backoff=1,
            delay=1,
        )


class ScheduleFromTaskTask(BaseExecutorTask[ScheduleFromTaskTaskInterface, Context]):
    cls_meta = ScheduleFromTaskTaskInterface

    async def run(self) -> TaskResult:
        await self._ctx.tp.schedule(SomeTaskInterface(foo="1"))
        return Success()


REGISTRY: TaskRegistry = TaskRegistry.create(
    [
        SomeTask,
        BrokenTask,
        RetryTask,
        TestIdsTask,
        ScheduleFromTaskTask,
    ]
)
