from typing import (
    Generic,
    TypeVar,
    cast,
)

import aiohttp.typedefs
import aiohttp.web
import attr
import pydantic
from typing_extensions import override

import dl_app_api_base.handlers as handlers
import dl_app_api_base.middlewares as middlewares
import dl_app_api_base.openapi as openapi
import dl_app_base
import dl_settings


class HttpServerSettings(dl_settings.BaseSettings):
    HOST: str
    PORT: int


class HttpServerAppSettingsMixin(dl_app_base.BaseAppSettings):
    HTTP_SERVER: HttpServerSettings = NotImplemented
    OPEN_API: openapi.OpenApiSettings = pydantic.Field(default_factory=openapi.OpenApiSettings)


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
                    access_log=None,
                ),
                name="run_http_server",
            ),
        )

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app(
        self,
    ) -> aiohttp.web.Application:
        app = aiohttp.web.Application(
            middlewares=await self._get_aiohttp_app_middlewares(),
        )

        await self._setup_routes(app)
        await self._setup_openapi(app)

        return app

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_middlewares(
        self,
    ) -> list[aiohttp.typedefs.Middleware]:
        logging_middleware = middlewares.LoggingMiddleware()
        error_handling_middleware = middlewares.ErrorHandlingMiddleware()

        return [
            logging_middleware.process,
            error_handling_middleware.process,
        ]

    async def _setup_routes(self, app: aiohttp.web.Application) -> None:
        routes = await self._get_aiohttp_app_routes()
        for route in routes:
            app.router.add_route(route.method, route.path, route.handler.process)

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_routes(
        self,
    ) -> list[handlers.Route]:
        result: list[handlers.Route] = []

        result.append(
            handlers.Route(
                method="GET",
                path="/api/v1/health/liveness",
                handler=handlers.LivenessProbeHandler(),
            ),
        )

        result.append(
            handlers.Route(
                method="GET",
                path="/api/v1/health/readiness",
                handler=handlers.ReadinessProbeHandler(
                    subsystems=await self._get_aiohttp_subsystem_readiness_callbacks(),
                ),
            ),
        )

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[handlers.SubsystemReadinessCallback]:
        return []

    async def _setup_openapi(self, app: aiohttp.web.Application) -> None:
        open_api_spec = await self._get_aiohttp_open_api_spec()
        open_api_handler = openapi.OpenApiHandler(raw_spec=open_api_spec.raw)
        app.router.add_route("GET", self.settings.OPEN_API.spec_path, open_api_handler.process)

        if self.settings.OPEN_API.SWAGGER_UI_ENABLED:
            swagger_handler = openapi.SwaggerHandler.from_dependencies(
                openapi.SwaggerHandlerDependencies(
                    url_prefix=self.settings.OPEN_API.DOCS_PATH,
                    config_url=self.settings.OPEN_API.SPEC_REL_URL,
                )
            )

            app.router.add_route("GET", self.settings.OPEN_API.DOCS_PATH, swagger_handler.process)
            app.router.add_static(f"{self.settings.OPEN_API.DOCS_PATH}/static/", path=swagger_handler.static_dir)

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_open_api_spec(
        self,
    ) -> openapi.OpenApiSpec:
        routes = cast(list[openapi.OpenApiRouteProtocol], await self._get_aiohttp_app_routes())
        return openapi.OpenApiSpec(routes=routes)
