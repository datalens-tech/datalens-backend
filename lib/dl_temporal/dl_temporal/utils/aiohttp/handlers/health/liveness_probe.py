import http
import logging

import aiohttp.web as aiohttp_web

import dl_temporal.utils.aiohttp.handlers as aiohttp_handlers


logger = logging.getLogger(__name__)


class LivenessProbeHandler:
    async def process(self, request: aiohttp_web.Request) -> aiohttp_web.Response:
        return aiohttp_handlers.Response.with_data(
            status=http.HTTPStatus.OK,
            data={"status": "healthy"},
        )


__all__ = [
    "LivenessProbeHandler",
]
