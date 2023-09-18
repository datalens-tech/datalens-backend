from __future__ import annotations

from typing import Optional

import attr

from bi_api_commons_ya_team.constants import DLCookiesYT
from dl_api_commons.base_models import AuthData
from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
    DLHeadersCommon,
)


@attr.s(frozen=True)
class YaTeamAuthData(AuthData):
    oauth_token: Optional[str] = attr.ib(default=None, repr=False)
    cookie_session_id: Optional[str] = attr.ib(default=None, repr=False)
    cookie_sessionid2: Optional[str] = attr.ib(default=None, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.oauth_token is None and self.cookie_session_id is None and self.cookie_sessionid2 is None:
            raise AssertionError("YaTeamAuthData must contain at least one of cookie or oAuth token")

    def get_headers(self) -> dict[DLHeaders, str]:
        if self.oauth_token is not None:
            return {DLHeadersCommon.AUTHORIZATION_TOKEN: f"OAuth {self.oauth_token}"}
        return {}

    def get_cookies(self) -> dict[DLCookies, str]:
        cookies: dict[DLCookiesYT, Optional[str]] = {
            DLCookiesYT.YA_TEAM_SESSION_ID: self.cookie_session_id,
            DLCookiesYT.YA_TEAM_SESSION_ID_2: self.cookie_sessionid2,
        }
        return {c_name: c_value for c_name, c_value in cookies.items() if c_value is not None}
