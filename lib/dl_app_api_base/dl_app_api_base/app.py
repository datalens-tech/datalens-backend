import re
from typing import (
    Generic,
    Iterator,
    Mapping,
    TypeVar,
)

import aiohttp.typedefs
import aiohttp.web
import attr
import pydantic
from typing_extensions import override

import dl_app_api_base.auth as auth
import dl_app_api_base.error_handling as error_handling
import dl_app_api_base.handlers as handlers
import dl_app_api_base.headers as headers
import dl_app_api_base.health as health
import dl_app_api_base.middlewares as middlewares
import dl_app_api_base.openapi as openapi
import dl_app_api_base.request_context as request_context
import dl_app_base
import dl_auth
import dl_dynconfig
import dl_settings


class HttpServerSettings(dl_settings.BaseSettings):
    HOST: str = "localhost"
    PORT: int = 8080


class AppInfoSettings(dl_settings.BaseSettings):
    NAME: str = "unknown"
    VERSION: str = "unknown"


class HttpServerAppSettingsMixin(dl_app_base.BaseAppSettings):
    # HTTP_SERVER will be ignored if running in Gunicorn
    HTTP_SERVER: HttpServerSettings = pydantic.Field(default_factory=HttpServerSettings)
    OPEN_API: openapi.OpenApiSettings = pydantic.Field(default_factory=openapi.OpenApiSettings)
    DYNCONFIG_SOURCE: dl_settings.TypedAnnotation[dl_dynconfig.BaseSourceSettings] = pydantic.Field(
        default_factory=dl_dynconfig.NullSourceSettings
    )
    APP_INFO: AppInfoSettings = pydantic.Field(default_factory=AppInfoSettings)


class HttpServerAppDynconfigMixin(dl_dynconfig.DynConfig):
    ...


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


HttpServerRequestContextProvider = request_context.RequestContextProvider[HttpServerRequestContext]
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
    async def _get_request_context_provider(
        self,
    ) -> HttpServerRequestContextProvider:
        return HttpServerRequestContextProvider()

    @dl_app_base.singleton_class_method_result
    async def _get_request_context_manager(
        self,
    ) -> HttpServerRequestContextManager:
        request_context_provider = await self._get_request_context_provider()
        return HttpServerRequestContextManager(
            context_factory=HttpServerRequestContext.factory,
            dependencies=HttpServerRequestContextDependencies(
                request_auth_checkers=await self._get_request_auth_checkers(),
                user_auth_provider_factories=await self._get_user_auth_provider_factories(),
            ),
            context_var=request_context_provider.context_var,
        )

    @dl_app_base.singleton_class_method_result
    async def _get_request_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            *await self._get_request_health_auth_checkers(),
            *await self._get_request_openapi_auth_checkers(),
            *await self._get_request_system_auth_checkers(),
            *await self._get_request_admin_auth_checkers(),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_health_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            auth.AlwaysAllowAuthChecker(
                route_matchers=await self._get_request_auth_checkers_health_route_matchers(),
                context_provider=await self._get_request_context_provider(),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_auth_checkers_health_route_matchers(
        self,
    ) -> list[auth.RouteMatcher]:
        return [
            # TODO: drop in BI-7161
            auth.RouteMatcher(
                path_regex=re.compile(r"^/api/v1/health/.*$"),
                methods=frozenset(["GET"]),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_openapi_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            auth.AlwaysDenyAuthChecker(
                route_matchers=await self._get_request_openapi_auth_checkers_route_matchers(),
                context_provider=await self._get_request_context_provider(),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_openapi_auth_checkers_route_matchers(
        self,
    ) -> list[auth.RouteMatcher]:
        return [
            auth.RouteMatcher(
                path_regex=re.compile(rf"^{self.settings.OPEN_API.DOCS_PATH}.*$"),
                methods=frozenset(["GET"]),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_system_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            auth.AlwaysAllowAuthChecker(
                route_matchers=await self._get_request_system_auth_checkers_route_matchers(),
                context_provider=await self._get_request_context_provider(),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_system_auth_checkers_route_matchers(
        self,
    ) -> list[auth.RouteMatcher]:
        return [
            auth.RouteMatcher(
                path_regex=re.compile(r"^/system/.*$"),
                methods=frozenset(["GET"]),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_admin_auth_checkers(
        self,
    ) -> list[auth.RequestAuthCheckerProtocol]:
        return [
            auth.AlwaysDenyAuthChecker(
                route_matchers=await self._get_request_admin_auth_checkers_route_matchers(),
                context_provider=await self._get_request_context_provider(),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_request_admin_auth_checkers_route_matchers(
        self,
    ) -> list[auth.RouteMatcher]:
        return [
            auth.RouteMatcher(
                path_regex=re.compile(r"^/admin/.*$"),
                methods=frozenset(["GET"]),
            ),
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_user_auth_provider_factories(
        self,
    ) -> dict[dl_auth.AuthTarget, auth.UserAuthProviderFactory]:
        return {}

    @dl_app_base.singleton_class_method_result
    async def _get_response_error_handler_map(
        self,
    ) -> Mapping[type[Exception], handlers.ErrorResponseSchema]:
        return error_handling.DEFAULT_ERROR_MAP

    @dl_app_base.singleton_class_method_result
    async def _get_response_error_handlers(
        self,
    ) -> list[error_handling.ErrorHandlerProtocol]:
        error_map = await self._get_response_error_handler_map()
        return [
            error_handling.MapErrorHandler(map=error_map).process,
        ]

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_middlewares(
        self,
    ) -> list[aiohttp.typedefs.Middleware]:
        request_context_provider = await self._get_request_context_provider()

        request_context_middlewares = request_context.RequestContextMiddleware(
            request_context_manager=await self._get_request_context_manager(),
        )
        logging_context_middleware = middlewares.LoggingContextMiddleware(
            request_context_provider=request_context_provider,
        )
        logging_middleware = middlewares.LoggingMiddleware(
            request_context_provider=request_context_provider,
        )
        error_handling_middleware = error_handling.ErrorHandlingMiddleware(
            error_handlers=await self._get_response_error_handlers(),
        )
        auth_middleware = auth.AuthMiddleware(
            request_context_provider=request_context_provider,
        )

        return [
            request_context_middlewares.process,
            logging_context_middleware.process,
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

        readiness_service = await self._get_aiohttp_readiness_service()
        liveness_handler = handlers.LivenessProbeHandler()
        readiness_handler = handlers.ReadinessProbeHandler(readiness_service=readiness_service)
        startup_handler = handlers.StartupProbeHandler(readiness_service=readiness_service)

        result.append(
            handlers.Route(
                method="GET",
                path="/system/health/liveness",
                handler=liveness_handler,
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/system/health/readiness",
                handler=readiness_handler,
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/system/health/startup",
                handler=startup_handler,
            ),
        )

        # TODO: drop in BI-7161
        result.append(
            handlers.Route(
                method="GET",
                path="/api/v1/health/liveness",
                handler=liveness_handler,
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/api/v1/health/readiness",
                handler=readiness_handler,
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/api/v1/health/startup",
                handler=startup_handler,
            ),
        )

        result.append(
            handlers.Route(
                method="GET",
                path="/system/app-info",
                handler=handlers.AppInfoHandler(
                    app_name=self.settings.APP_INFO.NAME,
                    version=self.settings.APP_INFO.VERSION,
                ),
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/admin/settings",
                handler=handlers.SettingsHandler(
                    settings_repr=self.settings.model_formatted_repr(),
                ),
            ),
        )
        result.append(
            handlers.Route(
                method="GET",
                path="/admin/dynconfig",
                handler=handlers.DynConfigHandler(
                    dynconfig=await self._get_dynconfig(),
                    source_type=self.settings.DYNCONFIG_SOURCE.type,
                ),
            ),
        )

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_dynconfig_source(
        self,
    ) -> dl_dynconfig.BaseSource:
        settings = self.settings.DYNCONFIG_SOURCE
        if isinstance(settings, dl_dynconfig.S3SourceSettings):
            return dl_dynconfig.S3Source.from_settings(settings)
        if isinstance(settings, dl_dynconfig.CachedS3SourceSettings):
            return dl_dynconfig.CachedS3Source.from_settings(settings)
        if isinstance(settings, dl_dynconfig.NullSourceSettings):
            return dl_dynconfig.NullSource.from_settings(settings)

        raise ValueError(f"Unknown dynconfig source type: {type(settings)}")

    @dl_app_base.singleton_class_method_result
    async def _get_dynconfig(
        self,
    ) -> HttpServerAppDynconfigMixin:
        return HttpServerAppDynconfigMixin.model_from_source(
            source=await self._get_dynconfig_source(),
        )

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[health.SubsystemReadinessCallback]:
        result: list[health.SubsystemReadinessCallback] = []

        source = await self._get_dynconfig_source()
        if not isinstance(source, dl_dynconfig.NullSource):
            result.append(
                health.SubsystemReadinessAsyncCallback(
                    name="dynconfig_source.check_readiness",
                    is_ready=source.check_readiness,
                ),
            )

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_readiness_service(
        self,
    ) -> health.ReadinessService:
        return health.ReadinessService(
            subsystems=await self._get_aiohttp_subsystem_readiness_callbacks(),
        )

    async def _setup_openapi(self, app: aiohttp.web.Application) -> None:
        open_api_spec = await self._get_aiohttp_open_api_spec()
        open_api_handler = openapi.OpenApiHandler(raw_spec=open_api_spec.raw)
        app.router.add_route("GET", self.settings.OPEN_API.SPEC_PATH, open_api_handler.process)

        if self.settings.OPEN_API.SWAGGER_UI_ENABLED:
            swagger_handler = openapi.SwaggerHandler.from_dependencies(
                openapi.SwaggerHandlerDependencies(
                    url_prefix=self.settings.OPEN_API.EXTERNAL_DOCS_PATH,
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
        return openapi.OpenApiSpec(
            routes=routes,
            external_route_prefix=self.settings.OPEN_API.EXTERNAL_ROUTE_PREFIX,
        )
