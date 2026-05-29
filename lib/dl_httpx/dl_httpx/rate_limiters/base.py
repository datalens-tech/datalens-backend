import contextlib
from typing import Protocol

import dl_httpx.exceptions as exceptions
import dl_settings


class RateLimitHttpxClientException(exceptions.BaseHttpxClientException): ...


class RateLimiterProtocol(Protocol):
    def context(self) -> contextlib.AbstractContextManager[None]: ...

    def context_async(self) -> contextlib.AbstractAsyncContextManager[None]: ...


class RateLimiterSettings(dl_settings.TypedBaseSettings): ...
