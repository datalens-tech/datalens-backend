import abc
import logging
from typing import Optional

import arq
import attr

from dl_task_processor.arq_wrapper import (
    ArqRedis,
    ArqRedisSettings,
    create_redis_pool,
)
from dl_task_processor.context import (
    BaseContext,
    BaseContextFabric,
)
from dl_task_processor.executor import (
    Executor,
    TaskInstance,
)
from dl_task_processor.state import (
    BITaskStateImpl,
    DummyStateImpl,
    TaskState,
)
from dl_task_processor.task import (
    BaseTaskMeta,
    InstanceID,
    Retry,
    TaskRegistry,
)
from dl_utils.aio import await_sync


LOGGER = logging.getLogger(__name__)


class BaseTaskProcessorImpl(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def schedule(self, task: TaskInstance) -> None:
        pass


class DummyTaskProcessorImpl(BaseTaskProcessorImpl):
    async def schedule(self, task: TaskInstance) -> None:
        pass


@attr.s
class ARQProcessorImpl(BaseTaskProcessorImpl):
    pool: arq.ArqRedis = attr.ib()

    async def schedule(self, task: TaskInstance) -> None:
        await self.pool.enqueue_job(
            "arq_base_task",
            dict(
                {
                    "name": task.name,
                    "instance_id": task.instance_id,
                    "request_id": task.request_id,
                    "task_params": task.params,
                },
            ),
        )


@attr.s
class LocalProcessorImpl(BaseTaskProcessorImpl):
    _executor: Executor = attr.ib()
    _is_sync: bool = attr.ib(default=False)

    async def schedule(self, task: TaskInstance) -> None:
        while True:
            result = await self._executor.run_job(task=task)
            if not isinstance(result, Retry):
                return
            task.attempt += 1


@attr.s
class TaskProcessor:
    _impl: BaseTaskProcessorImpl = attr.ib()
    _state: TaskState = attr.ib()
    _request_id: Optional[str] = attr.ib(default=None)

    async def schedule(self, task: BaseTaskMeta) -> TaskInstance:
        instance_id = InstanceID.make()
        task_instance = TaskInstance(
            name=task.name,
            instance_id=instance_id,
            params=task.get_params(),
            request_id=self._request_id,
        )
        LOGGER.info(
            "Schedule task %s with params %s as instance_id %s",
            task.name,
            task.get_params(),
            task_instance.instance_id.to_str(),
        )
        self._state.set_scheduled(task_instance)
        await self._impl.schedule(task_instance)
        return task_instance


def make_task_processor(redis_pool: arq.ArqRedis, request_id: Optional[str] = None) -> TaskProcessor:
    impl = ARQProcessorImpl(redis_pool)
    state = TaskState(DummyStateImpl())
    return TaskProcessor(impl=impl, state=state, request_id=request_id)


class TaskProcessorFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make(self, request_id: Optional[str] = None) -> TaskProcessor:
        pass

    def cleanup(self) -> None:
        return

    async def cleanup_async(self) -> None:
        return


@attr.s
class ARQTaskProcessorFactory(TaskProcessorFactory):
    _redis_pool_settings: ArqRedisSettings = attr.ib()

    _redis_pool: Optional[ArqRedis] = attr.ib(init=False, default=None)

    def setup(self) -> None:
        self._redis_pool = await_sync(create_redis_pool(self._redis_pool_settings))

    def make(self, request_id: Optional[str] = None) -> TaskProcessor:
        if self._redis_pool is None:
            self.setup()
        assert self._redis_pool is not None
        return make_task_processor(redis_pool=self._redis_pool, request_id=request_id)

    def cleanup(self) -> None:
        if self._redis_pool is not None:
            await_sync(self._redis_pool.close())

    async def cleanup_async(self) -> None:
        if self._redis_pool is not None:
            await self._redis_pool.close()


class DummyTaskProcessorFactory(TaskProcessorFactory):
    def make(self, req_id: Optional[str] = None) -> TaskProcessor:
        impl = DummyTaskProcessorImpl()
        state = TaskState(DummyStateImpl())
        return TaskProcessor(impl=impl, state=state, request_id=req_id)


@attr.s
class LocalTaskProcessorFactory(TaskProcessorFactory):
    _context_fab: BaseContextFabric = attr.ib()
    _registry: TaskRegistry = attr.ib()

    _context: Optional[BaseContext] = attr.ib(init=False, default=None)

    def setup(self) -> None:
        self._context = await_sync(self._context_fab.make())

    def make(self, request_id: Optional[str] = None) -> TaskProcessor:
        if self._context is None:
            self.setup()
        assert self._context is not None
        task_state = TaskState(BITaskStateImpl())
        executor = Executor(context=self._context, state=task_state, registry=self._registry)
        impl = LocalProcessorImpl(executor)
        processor = TaskProcessor(impl=impl, state=task_state, request_id=request_id)

        LOGGER.info("Local TP is ready")
        return processor

    def cleanup(self) -> None:
        if self._context is not None:
            await_sync(self._context_fab.tear_down(self._context))

    async def cleanup_async(self) -> None:
        if self._context is not None:
            await self._context_fab.tear_down(self._context)
