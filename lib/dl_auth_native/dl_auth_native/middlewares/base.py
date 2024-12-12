import typing

import attr

import dl_api_commons.base_models as dl_api_commons_base_models
import dl_auth_native.token as token
import dl_constants.api_constants as dl_constants_api_constants


@attr.s(frozen=True)
class AuthData(dl_api_commons_base_models.AuthData):
    _user_access_token: str = attr.ib()
    _token_type: str = attr.ib()

    def get_headers(
        self,
        target: typing.Optional[dl_api_commons_base_models.AuthTarget] = None,
    ) -> dict[dl_constants_api_constants.DLHeaders, str]:
        return {
            dl_constants_api_constants.DLHeadersCommon.AUTHORIZATION_TOKEN: f"{self._token_type} {self._user_access_token}",
        }

    def get_cookies(
        self,
        target: typing.Optional[dl_api_commons_base_models.AuthTarget] = None,
    ) -> dict[dl_constants_api_constants.DLCookies, str]:
        return {}


@attr.s(frozen=True)
class AuthResult:
    user_id: str = attr.ib()
    tenant: dl_api_commons_base_models.TenantCommon = attr.ib()
    auth_data: AuthData = attr.ib()


@attr.s()
class BaseMiddleware:
    _token_decoder: token.DecoderProtocol = attr.ib()
    _user_access_header_key: str = attr.ib(default=dl_api_commons_base_models.DLHeadersCommon.AUTHORIZATION_TOKEN)
    _token_type: str = attr.ib(default="Bearer")

    @attr.s(frozen=True)
    class Unauthorized(Exception):
        message: str = attr.ib()

    def _auth(self, user_access_header: typing.Optional[str]) -> AuthResult:
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
            tenant=dl_api_commons_base_models.TenantCommon(),
            auth_data=AuthData(
                user_access_token=user_access_token,
                token_type=self._token_type,
            ),
        )


__all__ = [
    "BaseMiddleware",
    "AuthResult",
    "AuthData",
]
