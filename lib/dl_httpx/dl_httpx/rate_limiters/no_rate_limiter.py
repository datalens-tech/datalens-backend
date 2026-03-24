from collections.abc import (
    AsyncIterator,
    Iterator,
)
import contextlib

from typing_extensions import Self

import dl_httpx.rate_limiters.base as base


class NoRateLimiterSettings(base.RateLimiterSettings):
    ...


base.RateLimiterSettings.register("NONE", NoRateLimiterSettings)


class NoRateLimiter:
    @classmethod
    def from_settings(cls, settings: NoRateLimiterSettings) -> Self:
        return cls()

    @contextlib.contextmanager
    def context(self) -> Iterator[None]:
        yield

    @contextlib.asynccontextmanager
    async def context_async(self) -> AsyncIterator[None]:
        yield
