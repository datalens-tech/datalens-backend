import abc
from typing import Protocol

import attrs

import dl_constants


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


@attrs.define(kw_only=True)
class USMasterTokenAuthProvider(AuthProviderProtocol):
    token: str

    def get_headers(self) -> dict[str, str]:
        return {dl_constants.DLHeadersCommon.US_MASTER_TOKEN.value: self.token}

    def get_cookies(self) -> dict[str, str]:
        return {}
