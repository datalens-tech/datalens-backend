import aiohttp.web
import attrs

import dl_app_api_base.handlers.base as base


@attrs.define(frozen=True, kw_only=True)
class SettingsHandler(base.BaseHandler):
    OPENAPI_TAGS = ["admin"]
    OPENAPI_DESCRIPTION = "Returns the full application settings"

    _settings_repr: str = attrs.field()

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return base.Response.with_plain_text(self._settings_repr)


__all__ = [
    "SettingsHandler",
]
