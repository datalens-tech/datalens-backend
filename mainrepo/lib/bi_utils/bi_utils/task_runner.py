from __future__ import annotations

import abc
import asyncio
from typing import (
    Any,
    Awaitable,
    Iterable,
    List,
)


class TaskRunner(abc.ABC):
    async def initialize(self) -> None:
        pass

    @abc.abstractmethod
    async def schedule(self, awaitable: Awaitable) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def finalize(self) -> Any:
        raise NotImplementedError


class ImmediateTaskRunner(TaskRunner):
    async def schedule(self, awaitable: Awaitable) -> None:
        await awaitable

    async def finalize(self) -> None:
        pass


class ConcurrentTaskRunner(TaskRunner):
    def __init__(self, concurrency_limit: int = 5) -> None:
        self._tasks: List[Awaitable] = []
        self._sem = asyncio.Semaphore(concurrency_limit)

    async def _semaphore_wrapper(self, awaitable: Awaitable) -> Any:
        async with self._sem:
            return await awaitable

    async def schedule(self, awaitable: Awaitable) -> None:
        self._tasks.append(self._semaphore_wrapper(awaitable))

    async def finalize(self) -> Iterable:  # should be list, but typing for asyncio.gather is a mess
        tasks = [asyncio.ensure_future(task) for task in self._tasks]  # creating task starts coroutine
        try:
            return await asyncio.gather(*tasks)
        except Exception:
            for task in tasks:
                task.cancel()
            raise
