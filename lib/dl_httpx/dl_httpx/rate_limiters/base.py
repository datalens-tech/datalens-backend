from typing import (
    AsyncContextManager,
    ContextManager,
    Protocol,
)

import dl_httpx.exceptions as exceptions
import dl_settings


class RateLimitHttpxClientException(exceptions.BaseHttpxClientException):
    ...


class RateLimiterProtocol(Protocol):
    def context(self) -> ContextManager[None]:
        ...

    def context_async(self) -> AsyncContextManager[None]:
        ...


class RateLimiterSettings(dl_settings.TypedBaseSettings):
    ...
