from aiohttp import web

from dl_auth_api_lib.oauth.google import GoogleOAuth
from dl_auth_api_lib.schemas import google as google_schemas
from dl_auth_api_lib.views.base import BaseView


class GoogleURIView(BaseView):
    client_cls = GoogleOAuth

    async def get(self) -> web.StreamResponse:
        data = google_schemas.GoogleUriRequestSchema().load(self.request.query)
        oauth_client = self.get_client(data)
        uri = oauth_client.get_auth_uri()
        return web.json_response(google_schemas.GoogleUriResponseSchema().dump(dict(uri=uri)))


class GoogleTokenView(BaseView):
    client_cls = GoogleOAuth

    async def post(self) -> web.StreamResponse:
        req_data = await self.request.json()
        data = google_schemas.GoogleTokenRequestSchema().load(req_data)
        oauth_client = self.get_client(data)
        resp_data = await oauth_client.get_auth_token(code=data["code"])
        if "error" in resp_data:
            return web.json_response(google_schemas.GoogleTokenErrorResponseSchema().dump(resp_data))
        return web.json_response(google_schemas.GoogleTokenResponseSchema().dump(resp_data))
