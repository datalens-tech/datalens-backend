import re
from typing import (
    Generic,
    Iterator,
    TypeVar,
)

import aiohttp.typedefs
import aiohttp.web
import attr
import pydantic
from typing_extensions import override

import dl_app_api_base.auth as auth
import dl_app_api_base.handlers as handlers
import dl_app_api_base.headers as headers
import dl_app_api_base.middlewares as middlewares
import dl_app_api_base.openapi as openapi
import dl_app_api_base.request_context as request_context
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
    _aiohttp_app: aiohttp.web.Application = attr.field(factory=aiohttp.web.Application)
    _aiohttp_app_host: str = attr.field()
    _aiohttp_app_port: int = attr.field()

    @property
    def aiohttp_app(self) -> aiohttp.web.Application:
        return self._aiohttp_app

    @override
    @property
    def main_callbacks(self) -> Iterator[dl_app_base.Callback]:
        yield from super().main_callbacks
        yield dl_app_base.Callback(
            coroutine=aiohttp.web._run_app(
                app=self.aiohttp_app,
                host=self._aiohttp_app_host,
                port=self._aiohttp_app_port,
                access_log=None,
            ),
            name="run_http_server",
        )

    @property
    def non_aiohttp_main_callbacks(self) -> Iterator[dl_app_base.Callback]:
        yield from super().main_callbacks


AppType = TypeVar("AppType", bound=HttpServerAppMixin)


@attr.define(frozen=True, kw_only=True)
class HttpServerRequestContextDependencies(
    auth.AuthRequestContextDependenciesMixin,
    request_context.BaseRequestContextDependencies,
):
    ...


class HttpServerRequestContext(
    headers.HeadersRequestContextMixin,
    auth.AuthRequestContextMixin,
    request_context.BaseRequestContext,
):
    _dependencies: HttpServerRequestContextDependencies


HttpServerRequestContextManager = request_context.BaseRequestContextManager[
    HttpServerRequestContextDependencies,
    HttpServerRequestContext,
]


@attr.define(kw_only=True, slots=False)
class HttpServerAppFactoryMixin(
    dl_app_base.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: HttpServerAppSettingsMixin

    @override
    async def _get_application(self) -> AppType:
        return self.app_class(
            startup_callbacks=await self._get_startup_callbacks(),
            shutdown_callbacks=await self._get_shutdown_callbacks(),
            main_callbacks=await self._get_main_callbacks(),
            logger=await self._get_logger(),
            aiohttp_app=await self._get_aiohttp_app(),
            aiohttp_app_host=self.settings.HTTP_SERVER.HOST,
            aiohttp_app_port=self.settings.HTTP_SERVER.PORT,
        )

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
    async def _get_request_context_manager(
        self,
    ) -> HttpServerRequestContextManager:
        return HttpServerRequestContextManager(
            context_factory=HttpServerRequestContext.factory,
            dependencies=HttpServerRequestContextDependencies(
                request_auth_checkers=await self._get_request_auth_checkers(),
            ),
        )

    @dl_app_base.singleton_class_method_result
    async def _get_request_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            auth.AlwaysAllowAuthChecker(
                route_matchers=[
                    auth.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/health/.*$"),
                        methods=frozenset(["GET"]),
                    ),
                    auth.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/docs/.*$"),
                        methods=frozenset(["GET"]),
                    ),
                ],
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_middlewares(
        self,
    ) -> list[aiohttp.typedefs.Middleware]:
        request_context_manager = await self._get_request_context_manager()

        request_context_middlewares = request_context.RequestContextMiddleware(
            request_context_manager=request_context_manager,
        )
        logging_middleware = middlewares.LoggingMiddleware(
            request_context_provider=request_context_manager,
        )
        error_handling_middleware = middlewares.ErrorHandlingMiddleware()
        auth_middleware = auth.AuthMiddleware(
            request_context_provider=request_context_manager,
        )

        return [
            request_context_middlewares.process,
            logging_middleware.process,
            error_handling_middleware.process,
            auth_middleware.process,
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
        routes = await self._get_aiohttp_app_routes()
        return openapi.OpenApiSpec(routes=routes)
