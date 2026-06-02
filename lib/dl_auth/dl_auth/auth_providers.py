import abc
from typing import (
    Annotated,
    Protocol,
    Self,
)

import attrs
import pydantic

import dl_auth.data as data
import dl_auth.dynamic_token as dynamic_token
import dl_constants
import dl_settings


class AuthProviderSettings(dl_settings.TypedBaseSettings): ...


class AuthProviderProtocol(Protocol):
    @abc.abstractmethod
    def get_headers(self) -> dict[str, str]: ...

    @abc.abstractmethod
    def get_cookies(self) -> dict[str, str]: ...

    async def get_headers_async(self) -> dict[str, str]: ...

    async def get_cookies_async(self) -> dict[str, str]: ...


class BaseAuthProvider(abc.ABC):
    @abc.abstractmethod
    def get_headers(self) -> dict[str, str]: ...

    @abc.abstractmethod
    def get_cookies(self) -> dict[str, str]: ...

    async def get_headers_async(self) -> dict[str, str]:
        return self.get_headers()

    async def get_cookies_async(self) -> dict[str, str]:
        return self.get_cookies()


class NoAuthProviderSettings(AuthProviderSettings): ...


AuthProviderSettings.register("NONE", NoAuthProviderSettings)


class NoAuthProvider(BaseAuthProvider):
    @classmethod
    def from_settings(cls, settings: NoAuthProviderSettings) -> Self:
        return cls()

    def get_headers(self) -> dict[str, str]:
        return {}

    def get_cookies(self) -> dict[str, str]:
        return {}


class OauthAuthProviderSettings(AuthProviderSettings):
    TOKEN: str = pydantic.Field(repr=False)


AuthProviderSettings.register("OAUTH", OauthAuthProviderSettings)


@attrs.define(kw_only=True)
class OauthAuthProvider(BaseAuthProvider):
    token: str

    @classmethod
    def from_settings(cls, settings: OauthAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": f"OAuth {self.token}"}

    def get_cookies(self) -> dict[str, str]:
        return {}


class BearerAuthProviderSettings(AuthProviderSettings):
    TOKEN: str = pydantic.Field(repr=False)


AuthProviderSettings.register("BEARER", BearerAuthProviderSettings)


@attrs.define(kw_only=True)
class BearerAuthProvider(BaseAuthProvider):
    token: str

    @classmethod
    def from_settings(cls, settings: BearerAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    def get_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def get_cookies(self) -> dict[str, str]:
        return {}


class USMasterTokenAuthProviderSettings(AuthProviderSettings):
    TOKEN: str = pydantic.Field(repr=False)


AuthProviderSettings.register("US_MASTER_TOKEN", USMasterTokenAuthProviderSettings)


@attrs.define(kw_only=True)
class USMasterTokenAuthProvider(BaseAuthProvider):
    token: str

    @classmethod
    def from_settings(cls, settings: USMasterTokenAuthProviderSettings) -> Self:
        return cls(token=settings.TOKEN)

    def get_headers(self) -> dict[str, str]:
        return {dl_constants.DLHeadersCommon.US_MASTER_TOKEN.value: self.token}

    def get_cookies(self) -> dict[str, str]:
        return {}


class AuthDataProviderSettings(AuthProviderSettings): ...


AuthProviderSettings.register("AUTH_DATA", AuthDataProviderSettings)


@attrs.define(kw_only=True, frozen=True)
class AuthDataAuthProvider(BaseAuthProvider):
    auth_data: data.AuthData
    target: data.AuthTarget

    def get_headers(self) -> dict[str, str]:
        return {k.value: v for k, v in self.auth_data.get_headers(self.target).items()}

    def get_cookies(self) -> dict[str, str]:
        return {k.value: v for k, v in self.auth_data.get_cookies(self.target).items()}


class USDynamicMasterTokenAuthProviderSettings(AuthProviderSettings):
    PRIVATE_KEY: Annotated[str, dl_settings.decode_multiline_validator] = pydantic.Field(repr=False)
    TOKEN_LIFETIME_SEC: int = 3600
    MIN_TTL_SEC: float = 900.0


AuthProviderSettings.register("US_DYNAMIC_MASTER_TOKEN", USDynamicMasterTokenAuthProviderSettings)


@attrs.define(kw_only=True)
class USDynamicMasterTokenAuthProvider(BaseAuthProvider):
    _generator: dynamic_token.DynamicMasterTokenGenerator

    @classmethod
    def from_settings(cls, settings: USDynamicMasterTokenAuthProviderSettings) -> Self:
        return cls(
            generator=dynamic_token.DynamicMasterTokenGenerator(
                private_key=settings.PRIVATE_KEY,
                token_lifetime_sec=settings.TOKEN_LIFETIME_SEC,
                min_ttl_sec=settings.MIN_TTL_SEC,
            ),
        )

    def get_headers(self) -> dict[str, str]:
        return {
            dl_constants.DLHeadersCommon.US_DYNAMIC_MASTER_TOKEN.value: self._generator.get_token(),
        }

    def get_cookies(self) -> dict[str, str]:
        return {}
