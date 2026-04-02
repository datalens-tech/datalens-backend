import contextlib
from typing import Protocol

import httpx

import dl_settings


class TransportAdapterProtocol(Protocol):
    def context(
        self,
        transport: httpx.BaseTransport,
        request: httpx.Request,
    ) -> contextlib.AbstractContextManager[httpx.BaseTransport]:
        ...

    def context_async(
        self,
        transport: httpx.AsyncBaseTransport,
        request: httpx.Request,
    ) -> contextlib.AbstractAsyncContextManager[httpx.AsyncBaseTransport]:
        ...


class BaseTransportAdapterSettings(dl_settings.TypedBaseSettings):
    ...
