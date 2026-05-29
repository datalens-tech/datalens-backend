from typing import Self

import dl_httpx


class UsEntriesClientException(Exception):
    pass


class EntryNotFoundError(UsEntriesClientException):
    @classmethod
    def from_httpx_exception(cls, exception: dl_httpx.HttpStatusHttpxClientException) -> Self:
        return cls()
