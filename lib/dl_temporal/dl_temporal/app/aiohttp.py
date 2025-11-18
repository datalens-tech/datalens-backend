from typing import (
    Generic,
    TypeVar,
)

import aiohttp.web
import attr
from typing_extensions import override

import dl_settings
import dl_temporal.utils.aiohttp.handlers as aiohttp_handlers
import dl_temporal.utils.aiohttp.printer as aiohttp_printer
import dl_temporal.utils.app as app_utils
import dl_temporal.utils.singleton as singleton_utils


class HttpServerSettings(dl_settings.BaseSettings):
    host: str
    port: int


class HttpServerAppSettingsMixin(app_utils.BaseAppSettings):
    http_server: HttpServerSettings = NotImplemented


@attr.define(frozen=True, kw_only=True)
class HttpServerAppMixin(app_utils.BaseApp):
    ...


AppType = TypeVar("AppType", bound=HttpServerAppMixin)


@attr.define(kw_only=True, slots=False)
class HttpServerAppFactoryMixin(
    app_utils.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: HttpServerAppSettingsMixin

    @override
    @singleton_utils.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[app_utils.Callback]:
        result = await super()._get_main_callbacks()

        result.append(
            app_utils.Callback(
                coroutine=aiohttp.web._run_app(
                    app=await self._get_aiohttp_app(),
                    host=self.settings.http_server.host,
                    port=self.settings.http_server.port,
                    print=aiohttp_printer.PrintLogger(),
                ),
                name="run_http_server",
            ),
        )

        return result

    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_app(
        self,
    ) -> aiohttp.web.Application:
        app = aiohttp.web.Application()
        app.add_routes(
            routes=await self._get_aiohttp_app_routes(),
        )
        return app

    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_app_routes(
        self,
    ) -> list[aiohttp.web.RouteDef]:
        result: list[aiohttp.web.RouteDef] = []

        aiohttp_liveness_probe_handler = await self._get_aiohttp_liveness_probe_handler()
        result.append(
            aiohttp.web.route(
                method="GET",
                path="/api/v1/health/liveness",
                handler=aiohttp_liveness_probe_handler.process,
            ),
        )

        aiohttp_readiness_probe_handler = await self._get_aiohttp_readiness_probe_handler()
        result.append(
            aiohttp.web.route(
                method="GET",
                path="/api/v1/health/readiness",
                handler=aiohttp_readiness_probe_handler.process,
            ),
        )

        return result

    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_liveness_probe_handler(
        self,
    ) -> aiohttp_handlers.LivenessProbeHandler:
        return aiohttp_handlers.LivenessProbeHandler()

    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_readiness_probe_handler(
        self,
    ) -> aiohttp_handlers.ReadinessProbeHandler:
        subsystems = await self._get_aiohttp_subsystem_readiness_callbacks()

        return aiohttp_handlers.ReadinessProbeHandler(
            subsystems=subsystems,
        )

    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[aiohttp_handlers.SubsystemReadinessCallback]:
        return []
