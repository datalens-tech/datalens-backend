import asyncio
import contextlib
import enum
import logging
import os
import subprocess
import tempfile
from typing import (
    AsyncGenerator,
    ClassVar,
    Generator,
    Protocol,
)

import aiohttp
import attr
import pytest
from typing_extensions import override

import dl_app_api_base
import dl_app_base


DIR_PATH = os.path.dirname(__file__)
LOGGER = logging.getLogger(__name__)
GUNICORN_HOST = "0.0.0.0"
GUNICORN_PORT = 54010


@attr.define(kw_only=True)
class CallbackCounter:
    dir_path: str
    name: str

    @property
    def path(self) -> str:
        return os.path.join(self.dir_path, f"{self.name}")

    def get(self) -> int:
        if not os.path.exists(self.path):
            return 0

        with open(self.path, "r") as f:
            return int(f.read())

    def set(self, value: int) -> None:
        with open(self.path, "w") as f:
            f.write(str(value))

    def increment(self) -> int:
        value = self.get() + 1
        self.set(value)
        return value


@pytest.fixture(name="callback_counters_path")
def fixture_callback_counters_path() -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture(name="startup_callback_counter")
def fixture_startup_callback_counter(callback_counters_path: str) -> CallbackCounter:
    return CallbackCounter(dir_path=callback_counters_path, name="startup")


@pytest.fixture(name="main_callback_counter")
def fixture_main_callback_counter(callback_counters_path: str) -> CallbackCounter:
    return CallbackCounter(dir_path=callback_counters_path, name="main")


@pytest.fixture(name="shutdown_callback_counter")
def fixture_shutdown_callback_counter(callback_counters_path: str) -> CallbackCounter:
    return CallbackCounter(dir_path=callback_counters_path, name="shutdown")


class CallbackSideEffect(enum.Enum):
    RAISE_EXCEPTION = "raise_exception"
    INFINITE_LOOP = "infinite_loop"
    NO_SIDE_EFFECT = "no_side_effect"


@attr.define(kw_only=True)
class Callback:
    counter: CallbackCounter
    side_effect: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT

    async def call(self) -> None:
        self.counter.increment()

        if self.side_effect == CallbackSideEffect.RAISE_EXCEPTION:
            raise RuntimeError("Test callback failure")
        elif self.side_effect == CallbackSideEffect.INFINITE_LOOP:
            while True:
                await asyncio.sleep(1)
        elif self.side_effect == CallbackSideEffect.NO_SIDE_EFFECT:
            pass


class AppSettings(dl_app_api_base.HttpServerAppSettingsMixin):
    CALLBACK_COUNTERS_PATH: str = NotImplemented
    STARTUP_CALLBACK_SIDE_EFFECT: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT
    MAIN_CALLBACK_SIDE_EFFECT: CallbackSideEffect = CallbackSideEffect.INFINITE_LOOP
    SHUTDOWN_CALLBACK_SIDE_EFFECT: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT


class App(dl_app_api_base.HttpServerAppMixin):
    ...


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
    async def _get_startup_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        return [
            dl_app_base.Callback(
                coroutine=Callback(
                    counter=CallbackCounter(dir_path=self.settings.CALLBACK_COUNTERS_PATH, name="startup"),
                    side_effect=self.settings.STARTUP_CALLBACK_SIDE_EFFECT,
                ).call(),
                name="startup",
            )
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        return [
            dl_app_base.Callback(
                coroutine=Callback(
                    counter=CallbackCounter(dir_path=self.settings.CALLBACK_COUNTERS_PATH, name="main"),
                    side_effect=self.settings.MAIN_CALLBACK_SIDE_EFFECT,
                ).call(),
                name="main",
            )
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_shutdown_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        return [
            dl_app_base.Callback(
                coroutine=Callback(
                    counter=CallbackCounter(dir_path=self.settings.CALLBACK_COUNTERS_PATH, name="shutdown"),
                    side_effect=self.settings.SHUTDOWN_CALLBACK_SIDE_EFFECT,
                ).call(),
                name="shutdown",
            )
        ]


async def get_app() -> App:
    settings = AppSettings()
    factory = AppFactory(settings=settings)
    return await factory.create_application()


@pytest.fixture(name="app")
def fixture_app() -> None:
    ...


class AppContextProtocol(Protocol):
    def __call__(
        self,
        startup_callback_side_effect: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT,
        main_callback_side_effect: CallbackSideEffect = CallbackSideEffect.INFINITE_LOOP,
        shutdown_callback_side_effect: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT,
    ) -> contextlib.AbstractAsyncContextManager[None]:
        ...


@pytest.fixture(name="app_context")
def fixture_app_context(
    monkeypatch: pytest.MonkeyPatch,
    callback_counters_path: str,
) -> AppContextProtocol:
    @contextlib.asynccontextmanager
    async def context(
        startup_callback_side_effect: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT,
        main_callback_side_effect: CallbackSideEffect = CallbackSideEffect.INFINITE_LOOP,
        shutdown_callback_side_effect: CallbackSideEffect = CallbackSideEffect.NO_SIDE_EFFECT,
    ) -> AsyncGenerator[None, None]:
        monkeypatch.setenv("CONFIG_PATH", os.path.join(DIR_PATH, "config.yaml"))
        monkeypatch.setenv("HTTP_SERVER__HOST", GUNICORN_HOST)
        monkeypatch.setenv("HTTP_SERVER__PORT", str(GUNICORN_PORT))
        monkeypatch.setenv("CALLBACK_COUNTERS_PATH", callback_counters_path)
        monkeypatch.setenv("STARTUP_CALLBACK_SIDE_EFFECT", startup_callback_side_effect.value)
        monkeypatch.setenv("MAIN_CALLBACK_SIDE_EFFECT", main_callback_side_effect.value)
        monkeypatch.setenv("SHUTDOWN_CALLBACK_SIDE_EFFECT", shutdown_callback_side_effect.value)

        gunicorn_process = subprocess.Popen(
            [
                "gunicorn",
                "dl_app_api_base_tests.unit.gunicorn.test_worker:get_app",
                "--bind",
                f"{GUNICORN_HOST}:{GUNICORN_PORT}",
                "--workers",
                "1",
                "--worker-class",
                "dl_app_api_base.GunicornWorker",
                "--log-config",
                os.path.join(DIR_PATH, "gunicorn_logging.ini"),
            ],
            cwd=DIR_PATH,
        )
        LOGGER.info("Waiting for gunicorn to start")
        await asyncio.sleep(2)
        LOGGER.info("Gunicorn started")
        try:
            yield
        finally:
            gunicorn_process.terminate()
            gunicorn_process.wait()

    return context


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    app_context: AppContextProtocol,
    startup_callback_counter: CallbackCounter,
    main_callback_counter: CallbackCounter,
    shutdown_callback_counter: CallbackCounter,
) -> None:
    assert startup_callback_counter.get() == 0
    assert main_callback_counter.get() == 0
    assert shutdown_callback_counter.get() == 0

    async with app_context():
        assert startup_callback_counter.get() == 1
        assert main_callback_counter.get() == 1
        assert shutdown_callback_counter.get() == 0

        response = await app_client.get("/api/v1/health/liveness")
        assert response.status == 200

    assert startup_callback_counter.get() == 1
    assert main_callback_counter.get() == 1
    assert shutdown_callback_counter.get() == 1


@pytest.mark.asyncio
async def test_startup_callback_failure(
    app_context: AppContextProtocol,
    startup_callback_counter: CallbackCounter,
    main_callback_counter: CallbackCounter,
    shutdown_callback_counter: CallbackCounter,
) -> None:
    assert startup_callback_counter.get() == 0
    assert main_callback_counter.get() == 0
    assert shutdown_callback_counter.get() == 0

    async with app_context(startup_callback_side_effect=CallbackSideEffect.RAISE_EXCEPTION):
        ...

    assert startup_callback_counter.get() > 1
    assert main_callback_counter.get() == 0
    assert shutdown_callback_counter.get() == 0


@pytest.mark.asyncio
async def test_main_callback_failure(
    app_context: AppContextProtocol,
    startup_callback_counter: CallbackCounter,
    main_callback_counter: CallbackCounter,
    shutdown_callback_counter: CallbackCounter,
) -> None:
    assert startup_callback_counter.get() == 0
    assert main_callback_counter.get() == 0
    assert shutdown_callback_counter.get() == 0

    async with app_context(main_callback_side_effect=CallbackSideEffect.RAISE_EXCEPTION):
        ...

    restart_count = startup_callback_counter.get()
    assert restart_count > 1
    assert main_callback_counter.get() == restart_count
    assert shutdown_callback_counter.get() == restart_count


@pytest.mark.asyncio
async def test_shutdown_callback_failure(
    app_context: AppContextProtocol,
    startup_callback_counter: CallbackCounter,
    main_callback_counter: CallbackCounter,
    shutdown_callback_counter: CallbackCounter,
) -> None:
    assert startup_callback_counter.get() == 0
    assert main_callback_counter.get() == 0
    assert shutdown_callback_counter.get() == 0

    async with app_context(shutdown_callback_side_effect=CallbackSideEffect.RAISE_EXCEPTION):
        assert startup_callback_counter.get() == 1
        assert main_callback_counter.get() == 1
        assert shutdown_callback_counter.get() == 0

    assert startup_callback_counter.get() == 1
    assert main_callback_counter.get() == 1
    assert shutdown_callback_counter.get() == 1
