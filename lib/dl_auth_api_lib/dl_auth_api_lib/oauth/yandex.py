from base64 import b64encode
from typing import Any
import urllib.parse

import aiohttp
import attr
import pydantic
from typing_extensions import Self

from dl_auth_api_lib.oauth.base import BaseOAuth
from dl_auth_api_lib.settings import BaseOAuthClient


_AUTH_URL: str = "https://oauth.yandex.ru/authorize?"
_TOKEN_URL: str = "https://oauth.yandex.ru/token"


class YandexOAuthClient(BaseOAuthClient):
    auth_type: str = "yandex"

    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str | None = pydantic.Field(default=None)
    auth_url: str = pydantic.Field(default=_AUTH_URL)
    token_url: str = pydantic.Field(default=_TOKEN_URL)


@attr.s
class YandexOAuth(BaseOAuth):
    client_id: str = attr.ib()
    client_secret: str = attr.ib()
    redirect_uri: str = attr.ib()
    scope: str | None = attr.ib(default=None)
    auth_url: str = attr.ib(default=_AUTH_URL)
    token_url: str = attr.ib(default=_TOKEN_URL)

    def get_auth_uri(self, origin: str | None = None) -> str:
        params = {
            "client_id": self.client_id,
            "redirect_uri": origin or self.redirect_uri,
            "response_type": "code",
            "scope": self.scope,
        }
        uri = self.auth_url + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return uri

    async def get_auth_token(self, code: str) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            headers=self._get_session_headers(),
        ) as session:
            token_data = {
                "grant_type": "authorization_code",
                "code": code,
            }
            async with session.post(self.token_url, data=token_data) as resp:
                token_response = await resp.json()
        return token_response

    @classmethod
    def from_settings(cls, settings: YandexOAuthClient) -> Self:
        return cls(
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            redirect_uri=settings.redirect_uri,
            scope=settings.scope,
            auth_url=settings.auth_url,
            token_url=settings.token_url,
        )

    def _get_session_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {b64encode(f'{self.client_id}:{self.client_secret}'.encode()).decode()}",
        }
        return headers
