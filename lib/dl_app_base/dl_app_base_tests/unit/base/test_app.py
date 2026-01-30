import asyncio
import logging

import attr
import pytest
from typing_extensions import override

import dl_app_base


LOGGER = logging.getLogger(__name__)


def get_active_tasks_count() -> int:
    return len(asyncio.all_tasks())


@attr.define(kw_only=True)
class Callback:
    call_count: int = attr.field(default=0)

    async def call(self) -> None:
        self.call_count += 1


class SleepingCallback(Callback):
    async def call(self) -> None:
        await super().call()
        while True:
            await asyncio.sleep(1)


class FailingCallback(Callback):
    async def call(self) -> None:
        await super().call()
        await asyncio.sleep(1)
        raise Exception


@pytest.mark.asyncio
async def test_default() -> None:
    startup_callback = Callback()
    main_callback = SleepingCallback()
    main2_callback = SleepingCallback()
    shutdown_callback = Callback()

    class TestAppFactory(dl_app_base.BaseAppFactory[dl_app_base.BaseApp]):
        settings = dl_app_base.BaseAppSettings()
        app_class = dl_app_base.BaseApp

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_logger(self) -> logging.Logger:
            return LOGGER

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_startup_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="startup", coroutine=startup_callback.call())]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_main_callbacks(self) -> list[dl_app_base.Callback]:
            return [
                dl_app_base.Callback(name="main", coroutine=main_callback.call()),
                dl_app_base.Callback(name="main2", coroutine=main2_callback.call()),
            ]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_shutdown_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="shutdown", coroutine=shutdown_callback.call())]

    factory = TestAppFactory(settings=dl_app_base.BaseAppSettings())
    app = await factory.create_application()

    before_start_tasks_count = get_active_tasks_count()

    assert startup_callback.call_count == 0
    assert main_callback.call_count == 0
    assert main2_callback.call_count == 0
    assert shutdown_callback.call_count == 0

    async with app.run_in_task_context() as app:
        assert startup_callback.call_count == 1
        assert main_callback.call_count == 1
        assert main2_callback.call_count == 1
        assert shutdown_callback.call_count == 0
        assert get_active_tasks_count() == before_start_tasks_count + 3  # main callbacks + run task

    assert startup_callback.call_count == 1
    assert main_callback.call_count == 1
    assert main2_callback.call_count == 1
    assert shutdown_callback.call_count == 1

    # Checking that all tasks are finished
    assert get_active_tasks_count() == before_start_tasks_count


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore: coroutine 'Callback.call' was never awaited:RuntimeWarning")
async def test_startup_callback_failure() -> None:
    startup_callback = FailingCallback()
    main_callback = Callback()
    main2_callback = Callback()
    shutdown_callback = Callback()

    class TestAppFactory(dl_app_base.BaseAppFactory[dl_app_base.BaseApp]):
        settings = dl_app_base.BaseAppSettings()
        app_class = dl_app_base.BaseApp

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_logger(self) -> logging.Logger:
            return LOGGER

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_startup_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="startup", coroutine=startup_callback.call())]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_main_callbacks(self) -> list[dl_app_base.Callback]:
            return [
                dl_app_base.Callback(name="main", coroutine=main_callback.call()),
                dl_app_base.Callback(name="main2", coroutine=main2_callback.call()),
            ]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_shutdown_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="shutdown", coroutine=shutdown_callback.call())]

    factory = TestAppFactory(settings=dl_app_base.BaseAppSettings())
    app = await factory.create_application()

    assert startup_callback.call_count == 0
    assert main_callback.call_count == 0
    assert main2_callback.call_count == 0
    assert shutdown_callback.call_count == 0

    with pytest.raises(dl_app_base.StartupError):
        await app.run()

    assert startup_callback.call_count == 1
    assert main_callback.call_count == 0
    assert main2_callback.call_count == 0
    assert shutdown_callback.call_count == 0


@pytest.mark.asyncio
async def test_main_callback_failure() -> None:
    startup_callback = Callback()
    main_callback = FailingCallback()
    main2_callback = SleepingCallback()
    shutdown_callback = Callback()

    class TestAppFactory(dl_app_base.BaseAppFactory[dl_app_base.BaseApp]):
        settings = dl_app_base.BaseAppSettings()
        app_class = dl_app_base.BaseApp

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_logger(self) -> logging.Logger:
            return LOGGER

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_startup_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="startup", coroutine=startup_callback.call())]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_main_callbacks(self) -> list[dl_app_base.Callback]:
            return [
                dl_app_base.Callback(name="main", coroutine=main_callback.call()),
                dl_app_base.Callback(name="main2", coroutine=main2_callback.call()),
            ]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_shutdown_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="shutdown", coroutine=shutdown_callback.call())]

    factory = TestAppFactory(settings=dl_app_base.BaseAppSettings())
    app = await factory.create_application()

    before_start_tasks_count = get_active_tasks_count()

    assert startup_callback.call_count == 0
    assert main_callback.call_count == 0
    assert main2_callback.call_count == 0
    assert shutdown_callback.call_count == 0

    with pytest.raises(dl_app_base.RunError):
        await app.run()

    assert startup_callback.call_count == 1
    assert main_callback.call_count == 1
    assert main2_callback.call_count == 1
    assert shutdown_callback.call_count == 1

    assert get_active_tasks_count() == before_start_tasks_count


@pytest.mark.asyncio
async def test_shutdown_callback_failure() -> None:
    startup_callback = Callback()
    main_callback = Callback()
    main2_callback = Callback()
    shutdown_callback = FailingCallback()

    class TestAppFactory(dl_app_base.BaseAppFactory[dl_app_base.BaseApp]):
        settings = dl_app_base.BaseAppSettings()
        app_class = dl_app_base.BaseApp

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_logger(self) -> logging.Logger:
            return LOGGER

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_startup_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="startup", coroutine=startup_callback.call())]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_main_callbacks(self) -> list[dl_app_base.Callback]:
            return [
                dl_app_base.Callback(name="main", coroutine=main_callback.call()),
                dl_app_base.Callback(name="main2", coroutine=main2_callback.call()),
            ]

        @override
        @dl_app_base.singleton_class_method_result
        async def _get_shutdown_callbacks(self) -> list[dl_app_base.Callback]:
            return [dl_app_base.Callback(name="shutdown", coroutine=shutdown_callback.call())]

    factory = TestAppFactory(settings=dl_app_base.BaseAppSettings())
    app = await factory.create_application()

    before_start_tasks_count = get_active_tasks_count()

    assert startup_callback.call_count == 0
    assert main_callback.call_count == 0
    assert main2_callback.call_count == 0
    assert shutdown_callback.call_count == 0

    with pytest.raises(dl_app_base.ShutdownError):
        await app.run()

    assert startup_callback.call_count == 1
    assert main_callback.call_count == 1
    assert main2_callback.call_count == 1
    assert shutdown_callback.call_count == 1

    assert get_active_tasks_count() == before_start_tasks_count
