from aiohttp import (
    hdrs,
    web,
)

from dl_auth_api_lib.oauth.yandex import YandexOAuth
from dl_auth_api_lib.schemas import yandex as yandex_schemas
from dl_auth_api_lib.views.base import BaseView


class YandexURIView(BaseView):
    client_cls = YandexOAuth

    async def get(self) -> web.StreamResponse:
        data = yandex_schemas.YandexUriRequestSchema().load(self.request.query)
        oauth_client = self.get_client(data)
        origin = self.request.headers.get(hdrs.ORIGIN)
        uri = oauth_client.get_auth_uri(origin=origin)
        return web.json_response(yandex_schemas.YandexUriResponseSchema().dump(dict(uri=uri)))


class YandexTokenView(BaseView):
    client_cls = YandexOAuth

    async def post(self) -> web.StreamResponse:
        req_data = await self.request.json()
        data = yandex_schemas.YandexTokenRequestSchema().load(req_data)
        oauth_client = self.get_client(data)
        resp_data = await oauth_client.get_auth_token(code=data["code"])
        if "error" in resp_data:
            return web.json_response(yandex_schemas.YandexTokenErrorResponseSchema().dump(resp_data))
        return web.json_response(yandex_schemas.YandexTokenResponseSchema().dump(resp_data))
