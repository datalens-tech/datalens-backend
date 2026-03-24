import collections
from collections.abc import (
    AsyncIterator,
    Iterator,
)
import contextlib
import threading
import time
from typing import Protocol

import attrs
import pydantic
from typing_extensions import Self

import dl_httpx.rate_limiters.base as base
import dl_httpx.utils.attrs as attrs_utils


class DateTimeProvider(Protocol):
    def get_now(self) -> float:
        ...


class DefaultDateTimeProvider:
    def get_now(self) -> float:
        return time.monotonic()


class SlidingWindowRateLimiterSettings(base.RateLimiterSettings):
    MAX_REQUESTS: int = pydantic.Field(gt=0)
    WINDOW_SECONDS: int = pydantic.Field(gt=0)


base.RateLimiterSettings.register(
    "SLIDING_WINDOW",
    SlidingWindowRateLimiterSettings,
)


@attrs.define(frozen=True, kw_only=True)
class SlidingWindowRateLimiter:
    _max_requests: int = attrs.field(
        validator=attrs_utils.field_must_be_positive,
    )
    _window_seconds: int = attrs.field(
        validator=attrs_utils.field_must_be_positive,
    )
    datetime_provider: DateTimeProvider = attrs.field(
        factory=DefaultDateTimeProvider,
    )
    _timestamps: collections.deque[float] = attrs.field(
        factory=collections.deque,
    )
    _lock: threading.Lock = attrs.field(
        factory=threading.Lock,
        init=False,
    )

    @classmethod
    def from_settings(cls, settings: SlidingWindowRateLimiterSettings) -> Self:
        return cls(
            max_requests=settings.MAX_REQUESTS,
            window_seconds=settings.WINDOW_SECONDS,
        )

    def _acquire_window_slot(self) -> None:
        now = self.datetime_provider.get_now()
        with self._lock:
            cutoff = now - self._window_seconds
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max_requests:
                raise base.RateLimitHttpxClientException()
            self._timestamps.append(now)

    @contextlib.contextmanager
    def context(self) -> Iterator[None]:
        self._acquire_window_slot()
        yield

    @contextlib.asynccontextmanager
    async def context_async(self) -> AsyncIterator[None]:
        self._acquire_window_slot()
        yield
