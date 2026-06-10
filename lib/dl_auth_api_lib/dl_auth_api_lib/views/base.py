from aiohttp import web

from dl_auth_api_lib.oauth.base import BaseOAuth


class BaseView[BASE_OAUTH_TV: BaseOAuth](web.View):
    client_cls: type[BASE_OAUTH_TV]

    def get_client(self, data: dict[str, str]) -> BASE_OAUTH_TV:
        conn_type = data["conn_type"]
        assert conn_type in self.request.app["clients"], f"Unknown connection type: {conn_type}"
        client_settings = self.request.app["clients"][conn_type]
        return self.client_cls.from_settings(client_settings)
