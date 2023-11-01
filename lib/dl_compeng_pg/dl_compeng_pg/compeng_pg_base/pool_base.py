from __future__ import annotations

import abc
from contextlib import asynccontextmanager
from typing import (
    AsyncGenerator,
    Type,
    TypeVar,
)


_POOL_WRAPPER_TV = TypeVar("_POOL_WRAPPER_TV", bound="BasePgPoolWrapper")

DEFAULT_POOL_MIN_SIZE = 2
DEFAULT_POOL_MAX_SIZE = 20
DEFAULT_OPERATION_TIMEOUT = 60.0


class BasePgPoolWrapper(metaclass=abc.ABCMeta):
    @classmethod
    async def connect(
        cls: Type[_POOL_WRAPPER_TV],
        url: str,
        pool_min_size: int = DEFAULT_POOL_MIN_SIZE,  # Initial pool size
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,  # Maximum pool size
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,  # SQL operation timeout
    ) -> _POOL_WRAPPER_TV:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    @classmethod
    @asynccontextmanager
    async def context(
        cls: Type[_POOL_WRAPPER_TV],
        url: str,
        pool_min_size: int = DEFAULT_POOL_MIN_SIZE,
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,
    ) -> AsyncGenerator[_POOL_WRAPPER_TV, None]:
        pool_wrapper = await cls.connect(
            url=url, pool_min_size=pool_min_size, pool_max_size=pool_max_size, operation_timeout=operation_timeout
        )
        try:
            yield pool_wrapper
        finally:
            await pool_wrapper.disconnect()
