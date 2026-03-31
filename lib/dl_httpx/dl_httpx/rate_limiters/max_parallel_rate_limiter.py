import contextlib
import threading
from typing import (
    AsyncIterator,
    Iterator,
    Literal,
)

import attrs
import pydantic
from typing_extensions import Self

import dl_httpx.rate_limiters.base as base
import dl_httpx.utils.attrs as attrs_utils


class MaxParallelRateLimiterSettings(base.RateLimiterSettings):
    type: Literal["MAX_PARALLEL"] = pydantic.Field(alias="TYPE", default="MAX_PARALLEL")
    MAX_PARALLEL: int = pydantic.Field(gt=0)


base.RateLimiterSettings.register(
    "MAX_PARALLEL",
    MaxParallelRateLimiterSettings,
)


@attrs.define(frozen=True, kw_only=True)
class MaxParallelRateLimiter:
    _max_parallel: int = attrs.field(
        validator=attrs_utils.field_must_be_positive,
    )
    _semaphore: threading.Semaphore = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: threading.Semaphore(self._max_parallel),
            takes_self=True,
        ),
    )

    @classmethod
    def from_settings(cls, settings: MaxParallelRateLimiterSettings) -> Self:
        return cls(max_parallel=settings.MAX_PARALLEL)

    @contextlib.contextmanager
    def context(self) -> Iterator[None]:
        acquired = self._semaphore.acquire(blocking=False)
        if not acquired:
            raise base.RateLimitHttpxClientException()
        try:
            yield
        finally:
            self._semaphore.release()

    @contextlib.asynccontextmanager
    async def context_async(self) -> AsyncIterator[None]:
        acquired = self._semaphore.acquire(blocking=False)
        if not acquired:
            raise base.RateLimitHttpxClientException()
        try:
            yield
        finally:
            self._semaphore.release()
