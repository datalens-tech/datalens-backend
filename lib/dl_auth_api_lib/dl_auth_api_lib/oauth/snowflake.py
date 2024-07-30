from base64 import b64encode
from typing import Any
import urllib.parse

import aiohttp
import attr
import pydantic
from typing_extensions import Self

from dl_auth_api_lib.oauth.base import BaseOAuth
from dl_auth_api_lib.settings import BaseOAuthClient


_AUTH_URL: str = "https://{account}.snowflakecomputing.com/oauth/authorize?"
_TOKEN_URL: str = "https://{account}.snowflakecomputing.com/oauth/token-request"


class SnowflakeOAuthClient(BaseOAuthClient):
    auth_type: str = "snowflake"
    conn_type: str = "snowflake"

    account: str
    client_id: str
    client_secret: str
    redirect_uri: str
    auth_url: str = pydantic.Field(default=_AUTH_URL)
    token_url: str = pydantic.Field(default=_TOKEN_URL)


@attr.s
class SnowflakeOAuth(BaseOAuth):
    account: str = attr.ib()
    client_id: str = attr.ib()
    client_secret: str = attr.ib()
    redirect_uri: str = attr.ib()
    auth_url: str = attr.ib(default=_AUTH_URL)
    token_url: str = attr.ib(default=_TOKEN_URL)

    def get_auth_uri(self, origin: str | None = None) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "state": "dl_conn_state",
        }
        uri = self.auth_url.format(account=self.account) + urllib.parse.urlencode(params)
        return uri

    async def get_auth_token(self, code: str, origin: str | None = None) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            headers=self._get_session_headers(),
        ) as session:
            token_data = {
                "grant_type": "authorization_code",
                "redirect_uri": self.redirect_uri,
                "code": code,
            }
            async with session.post(self.token_url.format(account=self.account), data=token_data) as resp:
                if resp.status >= 500:
                    resp.raise_for_status()
                token_response = await resp.json()
        return token_response

    @classmethod
    def from_settings(cls, settings: SnowflakeOAuthClient) -> Self:
        return cls(
            account=settings.account,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            redirect_uri=settings.redirect_uri,
            auth_url=settings.auth_url,
            token_url=settings.token_url,
        )

    def _get_session_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()}",
        }
        return headers
