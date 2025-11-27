import logging
from typing import Literal

import aiohttp.web

import dl_app_api_base.handlers.base as base
import dl_app_api_base.handlers.responses as responses


logger = logging.getLogger(__name__)


class LivenessProbeHandler(base.BaseHandler):
    OPENAPI_TAGS = ["health"]
    OPENAPI_DESCRIPTION = "Liveness probe, checks if the system is alive"

    class ResponseSchema(base.BaseResponseSchema):
        status: Literal["healthy"] = "healthy"

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return responses.Response.with_model(self.ResponseSchema())


__all__ = [
    "LivenessProbeHandler",
]
