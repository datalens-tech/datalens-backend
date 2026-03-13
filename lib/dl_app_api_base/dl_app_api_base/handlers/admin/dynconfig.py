from typing import Any

import aiohttp.web
import attrs

import dl_app_api_base.handlers.base as base
import dl_dynconfig


@attrs.define(frozen=True, kw_only=True)
class DynConfigHandler(base.BaseHandler):
    OPENAPI_TAGS = ["admin"]
    OPENAPI_DESCRIPTION = "Returns the current dynamic configuration and its source type"

    _dynconfig: dl_dynconfig.DynConfig = attrs.field()
    _source_type: str = attrs.field()

    class ResponseSchema(base.BaseResponseSchema):
        source_type: str
        config: dict[str, Any]

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return base.Response.with_model(
            self.ResponseSchema(
                source_type=self._source_type,
                config=self._dynconfig.model_dump(mode="json"),
            )
        )


__all__ = [
    "DynConfigHandler",
]
