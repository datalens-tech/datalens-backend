import http
import logging
import os
from typing import (
    AsyncGenerator,
    ClassVar,
)

import aiohttp
import aiohttp.web
import attr
import pytest
import pytest_asyncio
from typing_extensions import override

import dl_app_api_base.app
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


class App(dl_app_api_base.app.HttpServerAppMixin):
    ...


class AppSettings(dl_app_api_base.app.HttpServerAppSettingsMixin):
    readiness_resource: ReadinessResource


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
class AppFactory(dl_app_api_base.app.HttpServerAppFactoryMixin):
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
    async def _get_request_context_manager(  # type: ignore[override]
        self,
    ) -> RequestContextManager:
        return RequestContextManager(
            context_factory=RequestContext.factory,
            dependencies=RequestContextDependencies(
                counter=Counter(),
                value=42,
            ),
        )

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_app_routes(
        self,
    ) -> list[dl_app_api_base.Route]:
        routes = await super()._get_aiohttp_app_routes()
        routes.append(
            dl_app_api_base.Route(
                method="GET",
                path="/api/v1/counter",
                handler=CounterHandler(request_context_manager=await self._get_request_context_manager()),
            ),
        )
        return routes


@pytest.fixture(name="app_settings")
def fixture_app_settings(
    monkeypatch: pytest.MonkeyPatch,
    readiness_resource: ReadinessResource,
) -> AppSettings:
    monkeypatch.setenv("CONFIG_PATH", os.path.join(DIR_PATH, "config.yaml"))

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
