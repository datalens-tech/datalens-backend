import http
import logging
from typing import Literal

import aiohttp.web as aiohttp_web
import attr

import dl_app_api_base.handlers as handlers
from dl_app_api_base.health import (
    ReadinessService,
    SubsystemStatusSchema,
)


logger = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class ReadinessProbeHandler(handlers.BaseHandler):
    OPENAPI_TAGS = ["system"]
    OPENAPI_DESCRIPTION = "Readiness probe, checks if the system is ready to serve requests"

    class ResponseSchema(handlers.BaseResponseSchema):
        status: Literal["healthy", "unhealthy"]
        subsystems_status: dict[str, SubsystemStatusSchema]

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[handlers.BaseResponseSchema]]:
        return {
            **super()._response_schemas,
            http.HTTPStatus.INTERNAL_SERVER_ERROR: self.ResponseSchema,
        }

    readiness_service: ReadinessService

    async def process(self, request: aiohttp_web.Request) -> aiohttp_web.Response:
        subsystem_statuses = await self.readiness_service.get_all_statuses()
        status_values = {
            name: SubsystemStatusSchema.from_dataclass(s) for name, s in subsystem_statuses.statuses.items()
        }

        if subsystem_statuses.is_critical_ready():
            return handlers.Response.with_model(
                schema=self.ResponseSchema(status="healthy", subsystems_status=status_values),
                status=http.HTTPStatus.OK,
            )

        logger.error("Critical subsystems are unhealthy, status: %s", status_values)

        return handlers.Response.with_model(
            schema=self.ResponseSchema(status="unhealthy", subsystems_status=status_values),
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
        )


__all__ = [
    "ReadinessProbeHandler",
]
