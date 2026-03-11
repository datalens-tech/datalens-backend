import aiohttp.web
import attrs

import dl_app_api_base.handlers.base as base


@attrs.define(frozen=True, kw_only=True)
class AppInfoHandler(base.BaseHandler):
    OPENAPI_TAGS = ["system"]
    OPENAPI_DESCRIPTION = "Returns application info"

    _app_name: str = attrs.field()
    _version: str = attrs.field()

    class ResponseSchema(base.BaseResponseSchema):
        app_name: str
        version: str

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return base.Response.with_model(
            self.ResponseSchema(
                app_name=self._app_name,
                version=self._version,
            )
        )


__all__ = [
    "AppInfoHandler",
]
