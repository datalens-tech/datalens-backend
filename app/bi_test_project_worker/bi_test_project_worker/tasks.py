import asyncio
import logging

import attr

from bi_task_processor.task import (
    TaskRegistry,
    BaseExecutorTask,
    TaskResult,
    Success,
)
import bi_test_project_task_interface.tasks as task_interface
from bi_test_project_task_interface.context import Context


LOGGER = logging.getLogger(__name__)


@attr.s
class SomeTask(BaseExecutorTask[task_interface.SomeTask, Context]):
    cls_meta = task_interface.SomeTask

    async def run(self) -> TaskResult:
        LOGGER.info(f'Its some task with a {self._ctx} {self.meta.foo}')
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                self._ctx.tpe,
                lambda t: ', '.join((str(i) for i in range(t))),
                10,
            )
        except Exception as ex:
            LOGGER.exception(ex)
            raise
        LOGGER.info(result)
        return Success()


@attr.s
class AnotherTask(BaseExecutorTask[task_interface.AnotherTask, Context]):
    cls_meta = task_interface.AnotherTask

    async def run(self) -> TaskResult:
        LOGGER.info(f'Its another task with a {self._ctx} {self.meta.bar}')
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(
                self._ctx.tpe,
                lambda t: ', '.join((str(i) for i in range(t))),
                10,
            )
        except Exception as ex:
            LOGGER.exception(ex)
            raise
        LOGGER.info(result)
        return Success()


@attr.s
class BrokenTask(BaseExecutorTask[task_interface.BrokenTask, Context]):
    cls_meta = task_interface.BrokenTask

    async def run(self) -> TaskResult:
        LOGGER.info(f'Its broken task with a {self._ctx} {self.meta.foobar}')
        if int(1) == 1:  # hack mypy
            raise Exception('blabla')
        return Success()


@attr.s
class PeriodicTask(BaseExecutorTask[task_interface.PeriodicTask, Context]):
    cls_meta = task_interface.PeriodicTask

    async def run(self) -> TaskResult:
        LOGGER.info(f'Its periodic task with a {self._ctx} {self.meta}')
        return Success()


REGISTRY: TaskRegistry = TaskRegistry.create(
    tasks=[
        SomeTask,
        AnotherTask,
        BrokenTask,
        PeriodicTask,
    ],
)
