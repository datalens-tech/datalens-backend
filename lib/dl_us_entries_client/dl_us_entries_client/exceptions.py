from typing import Self

import dl_httpx


class UsEntriesClientError(Exception):
    pass


class NotFoundError(UsEntriesClientError):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientError) -> Self:
        return cls()


class EntryLockedError(UsEntriesClientError):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientError) -> Self:
        return cls()


class BadRequestError(UsEntriesClientError):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientError) -> Self:
        return cls()
