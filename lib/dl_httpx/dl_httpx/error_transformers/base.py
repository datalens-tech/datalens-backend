from typing import Protocol

import dl_httpx.exceptions as exceptions


class ErrorTransformerProtocol(Protocol):
    def transform(self, exception: exceptions.HttpStatusHttpxClientException) -> Exception | None:
        ...


class ExceptionFactoryProtocol(Protocol):
    def __call__(self, exception: exceptions.HttpStatusHttpxClientException) -> Exception:
        ...
