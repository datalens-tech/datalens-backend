import abc
import asyncio
from collections import defaultdict

import attr

from dl_task_processor.task import (
    InstanceID,
    TaskInstance,
)


# i will change it later
class BaseTaskStateImpl(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def set_state(self, task: TaskInstance, state: str) -> None:
        pass

    @abc.abstractmethod
    def get_state(self, task: TaskInstance) -> list:
        pass


class DummyStateImpl(BaseTaskStateImpl):
    def set_state(self, task: TaskInstance, state: str) -> None:
        pass

    def get_state(self, task: TaskInstance) -> list:
        return []


# i will change it later
@attr.s
class BITaskStateImpl(BaseTaskStateImpl):
    _states: dict[InstanceID, list[str]] = attr.ib(default=defaultdict(list))

    def set_state(self, task: TaskInstance, state: str) -> None:
        self._states[task.instance_id].append(state)

    def get_state(self, task: TaskInstance) -> list:
        return self._states[task.instance_id]


# i will change it later
@attr.s
class TaskState:
    _impl: BaseTaskStateImpl = attr.ib()

    def set_scheduled(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "scheduled")

    def set_started(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "started")

    def set_failed(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "failed")

    def set_retry(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "retry")

    def set_aborted(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "aborted")

    def set_success(self, task: TaskInstance) -> None:
        self._impl.set_state(task, "success")

    def get_state(self, task: TaskInstance) -> list:
        return self._impl.get_state(task)


# temporary task result checker for arq
# i will change it later
# maybe move to TaskState
async def wait_task(task: TaskInstance, state: TaskState, timeout: float = 10, interval: float = 0.5) -> list:
    """
    timeout == the bottom line
    """
    spent_time = 0.0
    while spent_time < timeout:
        current_state = state.get_state(task)
        # Has the task reached the final state?
        if {"success", "failed"} & set(state.get_state(task)):
            return current_state
        await asyncio.sleep(interval)
        spent_time += interval
    raise RuntimeError("Task was not complete in time")
