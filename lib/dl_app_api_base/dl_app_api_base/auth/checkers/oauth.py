from typing import Sequence

import attr
import pydantic
from typing_extensions import Self

import dl_app_api_base.auth.checkers.base as auth_checkers_base
import dl_app_api_base.auth.exc as auth_exc
import dl_app_api_base.auth.models as auth_models
import dl_app_api_base.request_context as request_context
import dl_constants
import dl_settings


@attr.define(frozen=True, kw_only=True)
class OAuthResult(auth_checkers_base.BaseRequestAuthResult):
    client_id: str


class OAuthUserSettings(dl_settings.BaseSettings):
    CLIENT_ID: str
    TOKEN: str = pydantic.Field(repr=False)


class OAuthCheckerSettings(dl_settings.BaseSettings):
    USERS: dict[str, OAuthUserSettings] = pydantic.Field(default_factory=dict)
    HEADER_KEY: str = dl_constants.DLHeadersCommon.AUTHORIZATION_TOKEN.value
    HEADER_PREFIX: str = dl_constants.DLAuthorizationHeaderPrefix.BEARER.value


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
        context_provider: request_context.RequestContextProviderProtocol,
    ) -> Self:
        return cls(
            route_matchers=route_matchers,
            context_provider=context_provider,
            token_to_result_map={user.TOKEN: OAuthResult(client_id=user.CLIENT_ID) for user in settings.USERS.values()},
            header_key=settings.HEADER_KEY,
            header_prefix=settings.HEADER_PREFIX,
        )

    async def is_applicable(self) -> bool:
        if not await super().is_applicable():
            return False

        context = self._context_provider.get()
        authorization_header = context.headers.get(self._header_key, None)
        if authorization_header is None:
            return False

        return authorization_header.startswith(self._header_prefix)

    async def check(self) -> OAuthResult:
        context = self._context_provider.get()
        authorization_header = context.headers.get(self._header_key, None)
        if authorization_header is None:
            raise auth_exc.AuthFailureError("Authorization header is required")

        if not authorization_header.startswith(self._header_prefix):
            raise auth_exc.AuthFailureError("Invalid authorization header format")

        token = authorization_header.removeprefix(self._header_prefix)
        user = self._token_to_result_map.get(token, None)
        if user is None:
            raise auth_exc.AuthFailureError("Invalid token")

        return user
