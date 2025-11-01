import base64
import json
from typing import Any

import attr

import dl_auth
import dl_constants


class ZitadelHeaders(dl_constants.DLHeaders):
    SERVICE_ACCESS_TOKEN = "x-dl-service-user-access-token"


@attr.s(frozen=True)
class ZitadelCookiesParser:
    @classmethod
    def from_raw(cls, raw_zitadel_cookie: str | None) -> dict[Any, Any] | None:
        if raw_zitadel_cookie is None:
            return None
        return json.loads(base64.b64decode(raw_zitadel_cookie))

    @classmethod
    def to_raw(cls, zitadel_cookie: dict[Any, Any]) -> str:
        return base64.b64encode(json.dumps(zitadel_cookie).encode("ascii")).decode("ascii")


class ZitadelCookies(dl_constants.DLCookies):
    ZITADEL_COOKIE = "zitadelCookie"


@attr.s(frozen=True)
class ZitadelAuthData(dl_auth.AuthData):
    _service_access_token: str = attr.ib()
    _user_access_token: str = attr.ib()

    def get_headers(
        self,
        target: dl_auth.AuthTarget | None = None,
    ) -> dict[dl_constants.DLHeaders, str]:
        return {
            ZitadelHeaders.SERVICE_ACCESS_TOKEN: self._service_access_token,
            dl_constants.DLHeadersCommon.AUTHORIZATION_TOKEN: f"Bearer {self._user_access_token}",
        }

    def get_cookies(
        self,
        target: dl_auth.AuthTarget | None = None,
    ) -> dict[dl_constants.DLCookies, str]:
        return {}


@attr.s(frozen=True)
class AuthResult:
    data: ZitadelAuthData = attr.ib()
    updated_cookies: dict[str, str] = attr.ib()
    user_id: str | None = attr.ib(default=None)
    user_name: str | None = attr.ib(default=None)
