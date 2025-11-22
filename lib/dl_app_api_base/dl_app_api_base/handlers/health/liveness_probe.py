import http
import logging

import aiohttp.web as aiohttp_web

import dl_app_api_base.handlers as handlers


logger = logging.getLogger(__name__)


class LivenessProbeHandler:
    async def process(self, request: aiohttp_web.Request) -> aiohttp_web.Response:
        return handlers.Response.with_data(
            status=http.HTTPStatus.OK,
            data={"status": "healthy"},
        )


__all__ = [
    "LivenessProbeHandler",
]
