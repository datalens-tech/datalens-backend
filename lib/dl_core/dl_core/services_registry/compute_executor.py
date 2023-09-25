from __future__ import annotations

import abc
import asyncio
from typing import (
    Callable,
    TypeVar,
)

from dl_utils.aio import ContextVarExecutor


_IDENTITY_TV = TypeVar("_IDENTITY_TV")


class ComputeExecutor:
    @abc.abstractmethod
    async def execute(self, func: Callable[[], _IDENTITY_TV]) -> _IDENTITY_TV:
        pass

    def close(self) -> None:
        pass


class ComputeExecutorTPE:
    def __init__(self, name_prefix: str = "COMPUTE_EXECUTOR_TPE_") -> None:
        self._tpe = ContextVarExecutor(thread_name_prefix=name_prefix)

    async def execute(self, func: Callable[[], _IDENTITY_TV]) -> _IDENTITY_TV:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._tpe, func)

    def close(self) -> None:
        self._tpe.shutdown(wait=False, cancel_futures=True)
