import abc
from typing import (
    Generic,
    TypeVar,
)

from typing_extensions import Self

from dl_auth_api_lib.settings import BaseOAuthClient


_TBaseOAuthClient = TypeVar("_TBaseOAuthClient", bound=BaseOAuthClient)


class BaseOAuth(Generic[_TBaseOAuthClient], abc.ABC):
    @abc.abstractmethod
    def get_auth_uri(self, origin: str | None = None) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_auth_token(self, code: str, origin: str | None = None) -> dict:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_settings(cls, settings: _TBaseOAuthClient) -> Self:
        raise NotImplementedError
