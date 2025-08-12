from typing import (
    Generic,
    TypeVar,
)

from aiohttp import web

from dl_auth_api_lib.oauth.base import BaseOAuth


_BASE_OAUTH_TV = TypeVar("_BASE_OAUTH_TV", bound=BaseOAuth)


class BaseView(web.View, Generic[_BASE_OAUTH_TV]):
    client_cls: type[_BASE_OAUTH_TV]

    def get_client(self, data: dict[str, str]) -> _BASE_OAUTH_TV:
        conn_type = data["conn_type"]
        assert conn_type in self.request.app["clients"], f"Unknown connection type: {conn_type}"
        client_settings = self.request.app["clients"][conn_type]
        return self.client_cls.from_settings(client_settings)
