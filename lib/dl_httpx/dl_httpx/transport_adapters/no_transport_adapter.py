import contextlib
from typing import (
    AsyncIterator,
    Iterator,
    Literal,
)

import httpx
import pydantic

import dl_httpx.transport_adapters.base as base


class NoTransportAdapterSettings(base.BaseTransportAdapterSettings):
    type: Literal["NONE"] = pydantic.Field(alias="TYPE", default="NONE")


base.BaseTransportAdapterSettings.register("NONE", NoTransportAdapterSettings)


class NoTransportAdapter:
    @contextlib.contextmanager
    def context(
        self,
        transport: httpx.BaseTransport,
        request: httpx.Request,
    ) -> Iterator[httpx.BaseTransport]:
        yield transport

    @contextlib.asynccontextmanager
    async def context_async(
        self,
        transport: httpx.AsyncBaseTransport,
        request: httpx.Request,
    ) -> AsyncIterator[httpx.AsyncBaseTransport]:
        yield transport
