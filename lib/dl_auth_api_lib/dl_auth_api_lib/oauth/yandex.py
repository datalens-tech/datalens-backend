from base64 import b64encode
from typing import Any
import urllib.parse

import aiohttp
import attr
from typing_extensions import Self

from dl_auth_api_lib.oauth.base import BaseOAuth
from dl_auth_api_lib.settings import YandexOAuthClient


@attr.s
class YandexOAuth(BaseOAuth):
    client_id: str = attr.ib()
    client_secret: str = attr.ib()
    redirect_uri: str = attr.ib()
    scope: str | None = attr.ib(default=None)

    _AUTH_URL: str = "https://oauth.yandex.ru/authorize?"
    _TOKEN_URL: str = "https://oauth.yandex.ru/token"

    def get_auth_uri(self) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": self.scope,
        }
        uri = self._AUTH_URL + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return uri

    async def get_auth_token(self, code: str) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            headers=self._get_session_headers(),
        ) as session:
            token_data = {
                "grant_type": "authorization_code",
                "code": code,
            }
            async with session.post(self._TOKEN_URL, data=token_data) as resp:
                token_response = await resp.json()
        return token_response

    @classmethod
    def from_settings(cls, settings: YandexOAuthClient) -> Self:
        return cls(
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            redirect_uri=settings.redirect_uri,
            scope=settings.scope,
        )

    def _get_session_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()}",
        }
        return headers
