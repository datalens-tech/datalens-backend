import http
import logging
import typing
from typing import Literal

import aiohttp.web as aiohttp_web
import attr

import dl_app_api_base.handlers as handlers


logger = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class SubsystemReadinessAsyncCallback:
    name: str
    is_ready: typing.Callable[[], typing.Awaitable[bool]]


@attr.define(frozen=True, kw_only=True)
class SubsystemReadinessSyncCallback:
    name: str
    is_ready: typing.Callable[[], bool]


SubsystemReadinessCallback = SubsystemReadinessAsyncCallback | SubsystemReadinessSyncCallback


@attr.define(frozen=True, kw_only=True)
class ReadinessProbeHandler(handlers.BaseHandler):
    OPENAPI_TAGS = ["health"]
    OPENAPI_DESCRIPTION = "Readiness probe, checks if the system is ready to serve requests"

    class ResponseSchema(handlers.BaseResponseSchema):
        status: Literal["healthy", "unhealthy"]
        subsystems_status: dict[str, bool]

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[handlers.BaseResponseSchema]]:
        return {
            **super()._response_schemas,
            http.HTTPStatus.INTERNAL_SERVER_ERROR: self.ResponseSchema,
        }

    subsystems: typing.Sequence[SubsystemReadinessCallback]

    async def _check_subsystem_readiness(self, subsystem: SubsystemReadinessCallback) -> bool:
        if isinstance(subsystem, SubsystemReadinessAsyncCallback):
            return await subsystem.is_ready()
        elif isinstance(subsystem, SubsystemReadinessSyncCallback):
            return subsystem.is_ready()
        else:
            raise ValueError(f"Unknown subsystem type: {type(subsystem)}")

    async def process(self, request: aiohttp_web.Request) -> aiohttp_web.Response:
        subsystems_status: dict[str, bool] = {
            subsystem.name: await self._check_subsystem_readiness(subsystem) for subsystem in self.subsystems
        }

        if all(subsystems_status.values()):
            return handlers.Response.with_model(
                schema=self.ResponseSchema(status="healthy", subsystems_status=subsystems_status),
                status=http.HTTPStatus.OK,
            )

        logger.error("Not all subsystems are healthy!", extra=subsystems_status)

        return handlers.Response.with_model(
            schema=self.ResponseSchema(status="unhealthy", subsystems_status=subsystems_status),
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
        )


__all__ = [
    "ReadinessProbeHandler",
    "SubsystemReadinessAsyncCallback",
]
