import http
import logging
import typing

import aiohttp.web as aiohttp_web
import attr

import dl_temporal.utils.aiohttp.handlers as aiohttp_handlers


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
class ReadinessProbeHandler:
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
            return aiohttp_handlers.Response.with_data(
                status=http.HTTPStatus.OK,
                data={"status": "healthy", "subsystems_status": subsystems_status},
            )

        logger.error("Not all subsystems are healthy!", extra=subsystems_status)

        return aiohttp_handlers.Response.with_data(
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            data={"status": "unhealthy", "subsystems_status": subsystems_status},
        )


__all__ = [
    "ReadinessProbeHandler",
    "SubsystemReadinessAsyncCallback",
]
