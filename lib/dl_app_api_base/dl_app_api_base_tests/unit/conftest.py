import http
import logging
import os
import re
from typing import (
    AsyncGenerator,
    ClassVar,
)

import aiohttp.web
import attr
import pytest
import pytest_asyncio
from typing_extensions import override

import dl_app_api_base
import dl_app_base


DIR_PATH = os.path.dirname(__file__)
LOGGER = logging.getLogger(__name__)


@attr.define(kw_only=True)
class ReadinessResource:
    ready: bool = attr.ib(default=True)

    def is_ready(self) -> bool:
        return self.ready

    def set_readiness(self, ready: bool) -> None:
        self.ready = ready


@pytest.fixture(name="readiness_resource")
def fixture_readiness_resource() -> ReadinessResource:
    return ReadinessResource()


@attr.define(kw_only=True, slots=False)
class Counter:
    value: int = attr.ib(default=0)

    def increment(self) -> int:
        self.value += 1
        return self.value


class App(dl_app_api_base.HttpServerAppMixin):
    ...


class HttpServerSettings(dl_app_api_base.HttpServerSettings):
    OAUTH_CHECKER: dl_app_api_base.OAuthCheckerSettings = NotImplemented


class AppSettings(dl_app_api_base.HttpServerAppSettingsMixin):
    readiness_resource: ReadinessResource
    HTTP_SERVER: HttpServerSettings = NotImplemented


@attr.define(kw_only=True, slots=False)
class CounterRequestContextDependenciesMixin(dl_app_api_base.BaseRequestContextDependencies):
    counter: Counter


@attr.define(kw_only=True, slots=False)
class CounterRequestContextMixin(dl_app_api_base.BaseRequestContext):
    _dependencies: CounterRequestContextDependenciesMixin

    @dl_app_base.singleton_class_method_result
    def get_counter_value(self) -> int:
        return self._dependencies.counter.increment()


@attr.define(kw_only=True, slots=False)
class ValueRequestContextDependenciesMixin(dl_app_api_base.BaseRequestContextDependencies):
    value: int


@attr.define(kw_only=True, slots=False)
class ValueRequestContextMixin(dl_app_api_base.BaseRequestContext):
    _dependencies: ValueRequestContextDependenciesMixin

    @dl_app_base.singleton_class_method_result
    def get_value(self) -> int:
        return self._dependencies.value


@attr.define(kw_only=True, slots=False)
class RequestContextDependencies(
    CounterRequestContextDependenciesMixin,
    ValueRequestContextDependenciesMixin,
    dl_app_api_base.HttpServerRequestContextDependencies,
):
    ...


@attr.define(kw_only=True, slots=False)
class RequestContext(
    CounterRequestContextMixin,
    ValueRequestContextMixin,
    dl_app_api_base.HttpServerRequestContext,
):
    _dependencies: RequestContextDependencies


RequestContextManager = dl_app_api_base.BaseRequestContextManager[
    RequestContextDependencies,
    RequestContext,
]


@attr.define(kw_only=True, slots=False)
class CounterHandler(dl_app_api_base.BaseHandler):
    request_context_manager: RequestContextManager

    class ResponseSchema(dl_app_api_base.BaseResponseSchema):
        counter_value: int
        value: int

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_context = self.request_context_manager.get()

        return dl_app_api_base.Response.with_model(
            schema=self.ResponseSchema(
                counter_value=request_context.get_counter_value(),
                value=request_context.get_value(),
            ),
            status=http.HTTPStatus.OK,
        )


@attr.define(kw_only=True, slots=False)
class PingHandler(dl_app_api_base.BaseHandler):
    class ResponseSchema(dl_app_api_base.BaseResponseSchema):
        message: str

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return dl_app_api_base.Response.with_model(
            schema=self.ResponseSchema(message="pong"),
            status=http.HTTPStatus.OK,
        )


@attr.define(kw_only=True, slots=False)
class AppFactory(dl_app_api_base.HttpServerAppFactoryMixin):
    settings: AppSettings
    app_class: ClassVar[type[App]] = App

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_logger(
        self,
    ) -> logging.Logger:
        return LOGGER

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[dl_app_api_base.SubsystemReadinessCallback]:
        return [
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="readiness_resource.is_ready",
                is_ready=self.settings.readiness_resource.is_ready,
            ),
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_request_auth_checkers(
        self,
    ) -> list[dl_app_api_base.RequestAuthCheckerProtocol]:
        base_checkers = await super()._get_request_auth_checkers()

        return [
            dl_app_api_base.AlwaysAllowAuthChecker(
                route_matchers=[
                    dl_app_api_base.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/counter"),
                        methods=frozenset(["GET"]),
                    ),
                ],
            ),
            dl_app_api_base.OAuthChecker.from_settings(
                settings=self.settings.HTTP_SERVER.OAUTH_CHECKER,
                route_matchers=[
                    dl_app_api_base.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/oauth/.*"),
                        methods=frozenset(["GET"]),
                    )
                ],
            ),
            dl_app_api_base.AlwaysAllowAuthChecker(
                route_matchers=[
                    dl_app_api_base.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/always_allow/.*"),
                        methods=frozenset(["GET"]),
                    ),
                ],
            ),
            dl_app_api_base.AlwaysDenyAuthChecker(
                route_matchers=[
                    dl_app_api_base.RouteMatcher(
                        path_regex=re.compile(r"^/api/v1/always_deny/.*"),
                        methods=frozenset(["GET"]),
                    ),
                ],
            ),
            *base_checkers,
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_request_context_manager(  # type: ignore[override]
        self,
    ) -> RequestContextManager:
        return RequestContextManager(
            context_factory=RequestContext.factory,
            dependencies=RequestContextDependencies(
                counter=Counter(),
                value=42,
                request_auth_checkers=await self._get_request_auth_checkers(),
            ),
        )

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_routes(
        self,
    ) -> list[dl_app_api_base.Route]:
        routes = await super()._get_aiohttp_app_routes()
        routes.extend(
            [
                dl_app_api_base.Route(
                    method="GET",
                    path="/api/v1/counter",
                    handler=CounterHandler(request_context_manager=await self._get_request_context_manager()),
                ),
                dl_app_api_base.Route(
                    method="GET",
                    path="/api/v1/oauth/ping",
                    handler=PingHandler(),
                ),
                dl_app_api_base.Route(
                    method="GET",
                    path="/api/v1/always_allow/ping",
                    handler=PingHandler(),
                ),
                dl_app_api_base.Route(
                    method="GET",
                    path="/api/v1/always_deny/ping",
                    handler=PingHandler(),
                ),
            ]
        )
        return routes


@pytest.fixture(name="oauth_user1_token")
def fixture_oauth_user1_token() -> str:
    return "user1_token"


@pytest.fixture(name="oauth_user2_token")
def fixture_oauth_user2_token() -> str:
    return "user2_token"


@pytest.fixture(name="app_settings")
def fixture_app_settings(
    monkeypatch: pytest.MonkeyPatch,
    readiness_resource: ReadinessResource,
    oauth_user1_token: str,
    oauth_user2_token: str,
) -> AppSettings:
    monkeypatch.setenv("CONFIG_PATH", os.path.join(DIR_PATH, "config.yaml"))

    monkeypatch.setenv("HTTP_SERVER__OAUTH_CHECKER__USERS__USER1__TOKEN", oauth_user1_token)
    monkeypatch.setenv("HTTP_SERVER__OAUTH_CHECKER__USERS__USER2__TOKEN", oauth_user2_token)

    return AppSettings(readiness_resource=readiness_resource)


@pytest_asyncio.fixture(name="app", autouse=True)
async def fixture_app(
    app_settings: AppSettings,
) -> AsyncGenerator[App, None]:
    factory = AppFactory(settings=app_settings)
    app = await factory.create_application()

    async with app.run_in_task_context() as app:
        yield app


@pytest_asyncio.fixture(name="app_client")
async def fixture_app_client(
    app_settings: AppSettings,
) -> AsyncGenerator[aiohttp.ClientSession, None]:
    async with aiohttp.ClientSession(
        base_url=f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}",
    ) as session:
        yield session
