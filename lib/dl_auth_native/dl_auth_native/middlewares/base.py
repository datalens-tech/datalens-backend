import attr
from typing_extensions import Self

import dl_api_commons
import dl_auth
import dl_auth_native.token as token
import dl_constants


@attr.s(frozen=True)
class AuthData(dl_auth.AuthData):
    _user_access_token: str = attr.ib()
    _token_type: str = attr.ib()
    _roles: list[str] = attr.ib(factory=list)

    def get_headers(
        self,
        target: dl_auth.AuthTarget | None = None,
    ) -> dict[dl_constants.DLHeaders, str]:
        return {
            dl_constants.DLHeadersCommon.AUTHORIZATION_TOKEN: f"{self._token_type} {self._user_access_token}",
        }

    def get_cookies(
        self,
        target: dl_auth.AuthTarget | None = None,
    ) -> dict[dl_constants.DLCookies, str]:
        return {}


@attr.s(frozen=True)
class AuthResult:
    user_id: str = attr.ib()
    tenant: dl_api_commons.TenantCommon = attr.ib()
    auth_data: AuthData = attr.ib()


@attr.s(frozen=True)
class MiddlewareSettings:
    decoder_key: str = attr.ib()
    decoder_algorithms: list[str] = attr.ib()


@attr.s()
class BaseMiddleware:
    _token_decoder: token.DecoderProtocol = attr.ib()
    _user_access_header_key: str = attr.ib(default=dl_constants.DLHeadersCommon.AUTHORIZATION_TOKEN)
    _token_type: str = attr.ib(default="Bearer")

    @classmethod
    def from_settings(cls, settings: MiddlewareSettings) -> Self:
        token_decoder = token.Decoder(
            key=settings.decoder_key,
            algorithms=settings.decoder_algorithms,
        )

        return cls(
            token_decoder=token_decoder,
        )

    @attr.s(frozen=True)
    class Unauthorized(Exception):
        message: str = attr.ib()

    def _auth(self, user_access_header: str | None) -> AuthResult:
        if user_access_header is None:
            raise self.Unauthorized("User access token header is missing")

        if not user_access_header.startswith(self._token_type):
            raise self.Unauthorized("Bad token type")

        user_access_token = user_access_header.removeprefix(self._token_type).strip()

        try:
            payload = self._token_decoder.decode(user_access_token)
        except token.TokenError as exc:
            raise self.Unauthorized(f"Invalid user access token: {exc.message}")

        return AuthResult(
            user_id=payload.user_id,
            tenant=dl_api_commons.TenantCommon(),
            auth_data=AuthData(
                user_access_token=user_access_token,
                token_type=self._token_type,
                roles=payload.roles,
            ),
        )


__all__ = [
    "AuthData",
    "AuthResult",
    "BaseMiddleware",
    "MiddlewareSettings",
]
