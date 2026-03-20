from __future__ import annotations

from typing import (
    ClassVar,
    Protocol,
)

import aiohttp.web
import attrs
import typing_extensions

import dl_auth


class USAuthProviderFactory(Protocol):
    def create(self, auth_data: dl_auth.AuthData) -> dl_auth.AuthProviderProtocol:
        ...


@attrs.define(frozen=True, kw_only=True)
class AuthDataUSAuthProvider:
    """Adapts AuthData to AuthProviderProtocol targeting UNITED_STORAGE."""

    _auth_data: dl_auth.AuthData

    def get_headers(self) -> dict[str, str]:
        return {k.value: v for k, v in self._auth_data.get_headers(dl_auth.AuthTarget.UNITED_STORAGE).items()}

    def get_cookies(self) -> dict[str, str]:
        return {k.value: v for k, v in self._auth_data.get_cookies(dl_auth.AuthTarget.UNITED_STORAGE).items()}

    async def get_headers_async(self) -> dict[str, str]:
        return self.get_headers()

    async def get_cookies_async(self) -> dict[str, str]:
        return self.get_cookies()


@attrs.define(kw_only=True)
class USAuthProviderFactoryService:
    APP_KEY: ClassVar[str] = "US_AUTH_PROVIDER_FACTORY_SERVICE"

    _factory: USAuthProviderFactory

    @property
    def factory(self) -> USAuthProviderFactory:
        return self._factory

    @classmethod
    def get_app_instance(cls, app: aiohttp.web.Application) -> typing_extensions.Self:
        service = app.get(cls.APP_KEY, None)
        if service is None:
            raise ValueError(f"{cls.__name__} was not initiated for application")

        assert isinstance(service, cls)
        return service

    def bind_to_app(self, app: aiohttp.web.Application) -> None:
        app[self.APP_KEY] = self
