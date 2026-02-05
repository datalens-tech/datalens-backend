from typing import Sequence

import aiohttp.web
import attr
import pydantic
from typing_extensions import Self

import dl_app_api_base.auth.checkers.base as auth_checkers_base
import dl_app_api_base.auth.exc as auth_exc
import dl_app_api_base.auth.models as auth_models
import dl_settings


@attr.define(frozen=True, kw_only=True)
class OAuthResult(auth_checkers_base.BaseRequestAuthResult):
    client_id: str


class OAuthUserSettings(dl_settings.BaseSettings):
    CLIENT_ID: str
    TOKEN: str = pydantic.Field(repr=False, alias="token")


class OAuthCheckerSettings(dl_settings.BaseSettings):
    USERS: dict[str, OAuthUserSettings] = pydantic.Field(default_factory=dict)
    HEADER_KEY: str = "Authorization"
    HEADER_PREFIX: str = "Bearer "


@attr.define(frozen=True, kw_only=True)
class OAuthChecker(auth_checkers_base.BaseRequestAuthChecker):
    _token_to_result_map: dict[str, OAuthResult]
    _header_key: str
    _header_prefix: str

    @classmethod
    def from_settings(
        cls,
        settings: OAuthCheckerSettings,
        route_matchers: Sequence[auth_models.RouteMatcher],
    ) -> Self:
        return cls(
            route_matchers=route_matchers,
            token_to_result_map={user.TOKEN: OAuthResult(client_id=user.CLIENT_ID) for user in settings.USERS.values()},
            header_key=settings.HEADER_KEY,
            header_prefix=settings.HEADER_PREFIX,
        )

    async def is_applicable(self, request: aiohttp.web.Request) -> bool:
        if not await super().is_applicable(request):
            return False

        authorization_header = request.headers.get(self._header_key, None)
        if authorization_header is None:
            return False

        return authorization_header.startswith(self._header_prefix)

    async def check(self, request: aiohttp.web.Request) -> OAuthResult:
        authorization_header = request.headers.get(self._header_key, None)
        if authorization_header is None:
            raise auth_exc.AuthFailureError("Authorization header is required")

        if not authorization_header.startswith(self._header_prefix):
            raise auth_exc.AuthFailureError("Invalid authorization header format")

        token = authorization_header.removeprefix(self._header_prefix)
        user = self._token_to_result_map.get(token, None)
        if user is None:
            raise auth_exc.AuthFailureError("Invalid token")

        return user
