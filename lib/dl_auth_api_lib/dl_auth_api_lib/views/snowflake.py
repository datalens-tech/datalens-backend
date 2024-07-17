from aiohttp import (
    hdrs,
    web,
)

from dl_auth_api_lib.oauth.snowflake import (
    SnowflakeOAuth,
    SnowflakeOAuthClient,
)
from dl_auth_api_lib.schemas import snowflake as snowflake_schemas


class BaseSnowflakeView(web.View):
    def get_client(self, data: dict[str, str]) -> SnowflakeOAuth:
        client_settings = SnowflakeOAuthClient(
            account=data["account"],
            client_id=data["client_id"],
            client_secret=data["client_secret"],
            redirect_uri=data["redirect_uri"],
        )
        return SnowflakeOAuth.from_settings(client_settings)


class SnowflakeURIView(BaseSnowflakeView):
    async def post(self) -> web.StreamResponse:
        req_data = await self.request.json()
        data = snowflake_schemas.SnowflakeUriRequestSchema().load(req_data)
        data["redirect_uri"] = self.request.headers.get(hdrs.ORIGIN)
        oauth_client = self.get_client(data)
        uri = oauth_client.get_auth_uri()
        return web.json_response(snowflake_schemas.SnowflakeUriResponseSchema().dump(dict(uri=uri)))


class SnowflakeTokenView(BaseSnowflakeView):
    async def post(self) -> web.StreamResponse:
        req_data = await self.request.json()
        data = snowflake_schemas.SnowflakeTokenRequestSchema().load(req_data)
        data["redirect_uri"] = self.request.headers.get(hdrs.ORIGIN)
        oauth_client = self.get_client(data)
        resp_data = await oauth_client.get_auth_token(code=data["code"])
        if "error" in resp_data:
            return web.json_response(snowflake_schemas.SnowflakeTokenErrorResponseSchema().dump(resp_data))
        return web.json_response(snowflake_schemas.SnowflakeTokenResponseSchema().dump(resp_data))
