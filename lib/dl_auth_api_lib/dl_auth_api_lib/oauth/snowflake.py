from base64 import b64encode
from typing import Any
import urllib.parse

import aiohttp
import attr


_AUTH_URL: str = "https://{account}.snowflakecomputing.com/oauth/authorize?"
_TOKEN_URL: str = "https://{account}.snowflakecomputing.com/oauth/token-request"


@attr.s
class SnowflakeOAuth:
    account: str = attr.ib()
    client_id: str = attr.ib()
    redirect_uri: str = attr.ib()
    auth_url: str = attr.ib(default=_AUTH_URL)
    token_url: str = attr.ib(default=_TOKEN_URL)

    def get_auth_uri(self) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "state": "dl_conn_state",
        }
        uri = self.auth_url.format(account=self.account) + urllib.parse.urlencode(params)
        return uri

    async def get_auth_token(self, code: str, client_secret: str) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            headers=self._get_session_headers(client_secret=client_secret),
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

    def _get_session_headers(self, client_secret: str) -> dict[str, str]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {b64encode(f'{self.client_id}:{client_secret}'.encode()).decode()}",
        }
        return headers
