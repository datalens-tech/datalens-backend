import abc
from typing import Protocol

import attrs


class AuthProviderProtocol(Protocol):
    @abc.abstractmethod
    def get_headers(self) -> dict[str, str]:
        ...

    @abc.abstractmethod
    def get_cookies(self) -> dict[str, str]:
        ...


class NoAuthProvider(AuthProviderProtocol):
    def get_headers(self) -> dict[str, str]:
        return {}

    def get_cookies(self) -> dict[str, str]:
        return {}


@attrs.define(kw_only=True)
class OauthAuthProvider(AuthProviderProtocol):
    token: str

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": f"OAuth {self.token}"}

    def get_cookies(self) -> dict[str, str]:
        return {}


__all__ = [
    "AuthProviderProtocol",
    "NoAuthProvider",
    "OauthAuthProvider",
]
