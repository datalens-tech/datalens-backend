import abc
from typing import Protocol

import attrs
from typing_extensions import Self

import dl_constants
import dl_settings


class AuthProviderSettings(dl_settings.TypedBaseSettings):
    ...


class AuthProviderProtocol(Protocol):
    @abc.abstractmethod
    def get_headers(self) -> dict[str, str]:
        ...

    @abc.abstractmethod
    def get_cookies(self) -> dict[str, str]:
        ...


class NoAuthProviderSettings(AuthProviderSettings):
    ...


AuthProviderSettings.register("NONE", NoAuthProviderSettings)


class NoAuthProvider(AuthProviderProtocol):
    @classmethod
    def from_settings(cls, settings: NoAuthProviderSettings) -> Self:
        return cls()

    def get_headers(self) -> dict[str, str]:
        return {}

    def get_cookies(self) -> dict[str, str]:
        return {}


class OauthAuthProviderSettings(AuthProviderSettings):
    TOKEN: str


AuthProviderSettings.register("OAUTH", OauthAuthProviderSettings)


@attrs.define(kw_only=True)
class OauthAuthProvider(AuthProviderProtocol):
    token: str

    @classmethod
    def from_settings(cls, settings: OauthAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": f"OAuth {self.token}"}

    def get_cookies(self) -> dict[str, str]:
        return {}


class USMasterTokenAuthProviderSettings(AuthProviderSettings):
    TOKEN: str


AuthProviderSettings.register("US_MASTER_TOKEN", USMasterTokenAuthProviderSettings)


@attrs.define(kw_only=True)
class USMasterTokenAuthProvider(AuthProviderProtocol):
    token: str

    @classmethod
    def from_settings(cls, settings: USMasterTokenAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    def get_headers(self) -> dict[str, str]:
        return {dl_constants.DLHeadersCommon.US_MASTER_TOKEN.value: self.token}

    def get_cookies(self) -> dict[str, str]:
        return {}
