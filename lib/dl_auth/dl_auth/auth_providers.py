import abc
from typing import Protocol

import attrs
import pydantic
from typing_extensions import (
    Self,
    override,
)

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

    async def get_headers_async(self) -> dict[str, str]:
        ...

    async def get_cookies_async(self) -> dict[str, str]:
        ...


@attrs.define(kw_only=True, frozen=True)
class BaseAuthProvider(abc.ABC):
    additional_headers: dict[str, str] = attrs.field(factory=dict)
    additional_cookies: dict[str, str] = attrs.field(factory=dict)

    def get_headers(self) -> dict[str, str]:
        return self.additional_headers.copy()

    def get_cookies(self) -> dict[str, str]:
        return self.additional_cookies.copy()

    async def get_headers_async(self) -> dict[str, str]:
        return self.get_headers()

    async def get_cookies_async(self) -> dict[str, str]:
        return self.get_cookies()


class NoAuthProviderSettings(AuthProviderSettings):
    ...


AuthProviderSettings.register("NONE", NoAuthProviderSettings)


class NoAuthProvider(BaseAuthProvider):
    @classmethod
    def from_settings(cls, settings: NoAuthProviderSettings) -> Self:
        return cls()


class OauthAuthProviderSettings(AuthProviderSettings):
    TOKEN: str = pydantic.Field(repr=False)


AuthProviderSettings.register("OAUTH", OauthAuthProviderSettings)


@attrs.define(kw_only=True, frozen=True)
class OauthAuthProvider(BaseAuthProvider):
    token: str

    @classmethod
    def from_settings(cls, settings: OauthAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    @override
    def get_headers(self) -> dict[str, str]:
        result = super().get_headers()
        result["Authorization"] = f"OAuth {self.token}"
        return result


class USMasterTokenAuthProviderSettings(AuthProviderSettings):
    TOKEN: str = pydantic.Field(repr=False)


AuthProviderSettings.register("US_MASTER_TOKEN", USMasterTokenAuthProviderSettings)


@attrs.define(kw_only=True, frozen=True)
class USMasterTokenAuthProvider(BaseAuthProvider):
    token: str

    @classmethod
    def from_settings(cls, settings: USMasterTokenAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    @override
    def get_headers(self) -> dict[str, str]:
        result = super().get_headers()
        result[dl_constants.DLHeadersCommon.US_MASTER_TOKEN.value] = self.token
        return result
