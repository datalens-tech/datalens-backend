import abc
import asyncio
import contextlib
import datetime
import enum
import logging
from typing import (
    AsyncGenerator,
    ClassVar,
    Generic,
    Iterator,
    TypeVar,
)

import attr
from typing_extensions import Self

import dl_settings
import dl_temporal.utils.app.exceptions as app_exceptions
import dl_temporal.utils.app.models as app_models
import dl_temporal.utils.singleton as singleton_utils


class BaseAppSettings(dl_settings.BaseRootSettings):
    ...


class RuntimeStatus(enum.Enum):
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


@attr.define(kw_only=True)
class AppState:
    runtime_status: RuntimeStatus = attr.field(default=RuntimeStatus.INITIALIZED)


@attr.define(frozen=True, kw_only=True)
class BaseApp:
    _startup_callbacks: list[app_models.Callback] = attr.field(factory=list)
    _shutdown_callbacks: list[app_models.Callback] = attr.field(factory=list)
    _main_callbacks: list[app_models.Callback] = attr.field(factory=list)

    logger: logging.Logger

    _state: AppState = attr.field(factory=AppState)

    @property
    def startup_callbacks(self) -> Iterator[app_models.Callback]:
        yield from self._startup_callbacks

    @property
    def shutdown_callbacks(self) -> Iterator[app_models.Callback]:
        yield from self._shutdown_callbacks

    @property
    def main_callbacks(self) -> Iterator[app_models.Callback]:
        yield from self._main_callbacks

    async def on_startup(self) -> None:
        self._state.runtime_status = RuntimeStatus.STARTING

        for callback in self.startup_callbacks:
            try:
                await callback.coroutine
            except Exception as e:
                message = f"Failed to startup due to failed StartupCallback({callback.name})"
                if callback.exception:
                    self.logger.exception(message)
                    raise app_exceptions.StartupError(message) from e
                else:
                    self.logger.warning(message)
            else:
                self.logger.info(f"Successfully started StartupCallback({callback.name})")

    async def on_shutdown(self) -> None:
        self._state.runtime_status = RuntimeStatus.STOPPING

        for callback in self.shutdown_callbacks:
            try:
                await callback.coroutine
            except Exception as e:
                message = f"Failed to shutdown due to failed ShutdownCallback({callback.name})"
                if callback.exception:
                    self.logger.exception(message)
                    raise app_exceptions.ShutdownError(message) from e
                else:
                    self.logger.warning(message)
            else:
                self.logger.info(f"Successfully shutdown ShutdownCallback({callback.name})")

        self._state.runtime_status = RuntimeStatus.STOPPED

    async def main(self) -> None:
        tasks: list[asyncio.Task[None]] = []
        for callback in self.main_callbacks:
            tasks.append(asyncio.create_task(callback.coroutine, name=callback.name))

        if len(tasks) == 0:
            self.logger.warning("No main callbacks provided")
            return

        self._state.runtime_status = RuntimeStatus.RUNNING

        try:
            for future in asyncio.as_completed(tasks):
                await future
                break
        except asyncio.CancelledError:
            self.logger.info("The main tasks execution was cancelled")
            raise
        except Exception as e:
            self.logger.exception("An error occurred during the main tasks execution")
            raise app_exceptions.RunError from e
        else:
            self.logger.error("Some tasks finished unexpectedly")
            raise app_exceptions.UnexpectedFinishError
        finally:
            finished_unexpectedly: list[asyncio.Task[None]] = []
            finished_with_exception: list[asyncio.Task[None]] = []
            unfinished_tasks: list[asyncio.Task[None]] = []

            for task in tasks:
                if task.done():
                    if task.exception() is None:
                        finished_unexpectedly.append(task)
                    else:
                        finished_with_exception.append(task)
                else:
                    unfinished_tasks.append(task)

            if finished_unexpectedly:
                self.logger.info("Tasks that finished unexpectedly:")
                for task in finished_unexpectedly:
                    self.logger.info("- %s", task.get_name())

            if finished_with_exception:
                self.logger.info("Tasks that finished with exception:")
                for task in finished_with_exception:
                    self.logger.info("- %s", task.get_name())

            if unfinished_tasks:
                self.logger.info("Unfinished tasks:")
                for task in unfinished_tasks:
                    self.logger.info("- %s - cancelling...", task.get_name())
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        self.logger.info("- %s - cancelled", task.get_name())

    async def run(self) -> None:
        await self.on_startup()

        try:
            await self.main()
        finally:
            await self.on_shutdown()

    @contextlib.asynccontextmanager
    async def run_in_task_context(
        self,
        readiness_timeout: datetime.timedelta = datetime.timedelta(seconds=60),
    ) -> AsyncGenerator[Self, None]:
        try:
            run_task = asyncio.create_task(self.run(), name="run_in_task_context")

            deadline = datetime.datetime.now() + readiness_timeout
            while datetime.datetime.now() < deadline and self._state.runtime_status != RuntimeStatus.RUNNING:
                if self._state.runtime_status in [RuntimeStatus.STOPPING, RuntimeStatus.STOPPED]:
                    raise RuntimeError("Failed to wait for the application to be running")

                self.logger.info("Waiting for the application to be running")
                await asyncio.sleep(1)

            yield self
        finally:
            self.logger.info("Cancelling the run task")
            run_task.cancel()
            try:
                await run_task
            except asyncio.CancelledError:
                self.logger.info("The run task was cancelled")


AppType = TypeVar("AppType", bound=BaseApp)


@attr.define(kw_only=True, slots=False)
class BaseAppFactory(Generic[AppType]):
    settings: BaseAppSettings
    app_class: ClassVar[type[AppType]]  # type: ignore

    async def create_application(
        self,
    ) -> AppType:
        return self.app_class(
            startup_callbacks=await self._get_startup_callbacks(),
            shutdown_callbacks=await self._get_shutdown_callbacks(),
            main_callbacks=await self._get_main_callbacks(),
            logger=await self._get_logger(),
        )

    @singleton_utils.singleton_class_method_result
    async def _get_startup_callbacks(
        self,
    ) -> list[app_models.Callback]:
        return []

    @singleton_utils.singleton_class_method_result
    async def _get_shutdown_callbacks(
        self,
    ) -> list[app_models.Callback]:
        return []

    @singleton_utils.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[app_models.Callback]:
        return []

    @abc.abstractmethod
    @singleton_utils.singleton_class_method_result
    async def _get_logger(
        self,
    ) -> logging.Logger:
        ...
