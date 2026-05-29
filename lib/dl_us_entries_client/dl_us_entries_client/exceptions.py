from typing import Self

import dl_httpx


class UsEntriesClientException(Exception):
    pass


class NotFoundError(UsEntriesClientException):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls()


class EntryLockedError(UsEntriesClientException):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls()


class BadRequest(UsEntriesClientException):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls()
