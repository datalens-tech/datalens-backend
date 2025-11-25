from typing import (
    Generic,
    TypeVar,
)

import aiohttp.web
import attr
from typing_extensions import override

import dl_app_api_base.handlers as handlers
import dl_app_api_base.printer as printer
import dl_app_base
import dl_settings


class HttpServerSettings(dl_settings.BaseSettings):
    HOST: str
    PORT: int


class HttpServerAppSettingsMixin(dl_app_base.BaseAppSettings):
    HTTP_SERVER: HttpServerSettings = NotImplemented


@attr.define(frozen=True, kw_only=True)
class HttpServerAppMixin(dl_app_base.BaseApp):
    ...


AppType = TypeVar("AppType", bound=HttpServerAppMixin)


@attr.define(kw_only=True, slots=False)
class HttpServerAppFactoryMixin(
    dl_app_base.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: HttpServerAppSettingsMixin

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        result = await super()._get_main_callbacks()

        result.append(
            dl_app_base.Callback(
                coroutine=aiohttp.web._run_app(
                    app=await self._get_aiohttp_app(),
                    host=self.settings.HTTP_SERVER.HOST,
                    port=self.settings.HTTP_SERVER.PORT,
                    print=printer.PrintLogger(),
                ),
                name="run_http_server",
            ),
        )

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app(
        self,
    ) -> aiohttp.web.Application:
        app = aiohttp.web.Application()
        app.add_routes(
            routes=await self._get_aiohttp_app_routes(),
        )
        return app

    @dl_app_base.singleton_class_method_result
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

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_liveness_probe_handler(
        self,
    ) -> handlers.LivenessProbeHandler:
        return handlers.LivenessProbeHandler()

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_readiness_probe_handler(
        self,
    ) -> handlers.ReadinessProbeHandler:
        subsystems = await self._get_aiohttp_subsystem_readiness_callbacks()

        return handlers.ReadinessProbeHandler(
            subsystems=subsystems,
        )

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[handlers.SubsystemReadinessCallback]:
        return []
