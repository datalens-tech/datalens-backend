import base64
import json
import typing

import attr

import dl_api_commons.base_models as dl_api_commons_base_models
import dl_constants.api_constants as dl_constants_api_constants


class ZitadelHeaders(dl_constants_api_constants.DLHeaders):
    SERVICE_ACCESS_TOKEN = "x-dl-service-user-access-token"


@attr.s(frozen=True)
class ZitadelCookiesParser:
    @classmethod
    def from_raw(cls, raw_zitadel_cookie: str | None) -> dict[typing.Any, typing.Any] | None:
        if raw_zitadel_cookie is None:
            return None
        return json.loads(base64.b64decode(raw_zitadel_cookie))

    @classmethod
    def to_raw(cls, zitadel_cookie: dict[typing.Any, typing.Any]) -> str:
        return base64.b64encode(json.dumps(zitadel_cookie).encode("ascii")).decode("ascii")


class ZitadelCookies(dl_constants_api_constants.DLCookies):
    ZITADEL_COOKIE = "zitadelCookie"


@attr.s(frozen=True)
class ZitadelAuthData(dl_api_commons_base_models.AuthData):
    _service_access_token: str = attr.ib()
    _user_access_token: str = attr.ib()

    def get_headers(
        self,
        target: typing.Optional[dl_api_commons_base_models.AuthTarget] = None,
    ) -> dict[dl_constants_api_constants.DLHeaders, str]:
        return {
            ZitadelHeaders.SERVICE_ACCESS_TOKEN: self._service_access_token,
            dl_constants_api_constants.DLHeadersCommon.AUTHORIZATION_TOKEN: f"Bearer {self._user_access_token}",
        }

    def get_cookies(
        self,
        target: typing.Optional[dl_api_commons_base_models.AuthTarget] = None,
    ) -> dict[dl_constants_api_constants.DLCookies, str]:
        return {}


@attr.s(frozen=True)
class AuthResult:
    data: ZitadelAuthData = attr.ib()
    updated_cookies: dict[str, str] = attr.ib()
    user_id: typing.Optional[str] = attr.ib(default=None)
    user_name: typing.Optional[str] = attr.ib(default=None)
