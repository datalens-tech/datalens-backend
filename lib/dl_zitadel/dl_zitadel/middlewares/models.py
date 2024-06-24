import typing

import attr

import dl_api_commons.base_models as dl_api_commons_base_models
import dl_constants.api_constants as dl_constants_api_constants


class ZitadelHeaders(dl_constants_api_constants.DLHeaders):
    SERVICE_ACCESS_TOKEN = "x-dl-service-user-access-token"


@attr.s(frozen=True)
class ZitadelAuthData(dl_api_commons_base_models.AuthData):
    _service_access_token: str
    _user_access_token: str

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
