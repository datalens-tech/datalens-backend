import asyncio
import logging
import os
import sys
from typing import (
    Any,
    Iterable,
)

import aiohttp.web
import gunicorn.workers.base

import dl_app_api_base.app as app
import dl_app_base


LOGGER = logging.getLogger(__name__)


class GunicornWorker(gunicorn.workers.base.Worker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.servers: list[asyncio.AbstractServer] = []
        self.exit_code = os.EX_OK
        self._runner: aiohttp.web.AppRunner | None = None
        self._application: app.HttpServerAppMixin | None = None
        self._non_aiohttp_main_tasks: list[asyncio.Task[None]] = []

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            pass
        finally:
            loop.run_until_complete(self._cleanup())
            loop.close()

        sys.exit(self.exit_code)

    async def _run(self) -> None:
        application = await self._create_application()
        await application.on_startup()

        await self._start_aiohttp_server(application.aiohttp_app)
        await self._start_non_aiohttp_main_tasks(application.non_aiohttp_main_callbacks)

        self._application = application
        self._application.state.runtime_status = dl_app_base.RuntimeStatus.RUNNING

        while self.alive:
            self.notify()
            await self._check_non_aiohttp_main_tasks()
            await asyncio.sleep(1)

    async def _create_application(self) -> app.HttpServerAppMixin:
        application_factory = self.wsgi
        if not callable(application_factory):
            raise RuntimeError(f"Application must be a callable, got {type(application_factory)}")

        LOGGER.info("Creating application...")
        if asyncio.iscoroutinefunction(application_factory):
            application = await application_factory()
        else:
            application = application_factory()

        if not isinstance(application, app.HttpServerAppMixin):
            raise RuntimeError(f"Application must be an instance of HttpServerAppMixin, got {type(application)}")

        return application

    async def _start_aiohttp_server(self, aiohttp_app: aiohttp.web.Application) -> None:
        LOGGER.info("Creating aiohttp.web.AppRunner...")
        self._runner = aiohttp.web.AppRunner(
            aiohttp_app,
            access_log=self.log.access_log if self.cfg.accesslog else None,
            access_log_format=self.cfg.access_log_format,
        )
        await self._runner.setup()

        LOGGER.info("Creating TCP sites for each configured socket...")
        for sock in self.sockets:
            site = aiohttp.web.SockSite(
                self._runner,
                sock,
                shutdown_timeout=self.cfg.graceful_timeout,
            )
            await site.start()
            if site._server is None:
                raise RuntimeError("Server is not started")
            self.servers.append(site._server)

    async def _start_non_aiohttp_main_tasks(self, non_aiohttp_main_callbacks: Iterable[dl_app_base.Callback]) -> None:
        for callback in non_aiohttp_main_callbacks:
            LOGGER.info("Creating task for MainCallback(%s)", callback.name)
            task = asyncio.create_task(callback.coroutine, name=callback.name)
            self._non_aiohttp_main_tasks.append(task)

    async def _check_non_aiohttp_main_tasks(self) -> None:
        done_tasks = [task for task in self._non_aiohttp_main_tasks if task.done()]
        if not done_tasks:
            return

        for task in done_tasks:
            exception = task.exception()
            if exception is None:
                LOGGER.error("Task %s finished unexpectedly", task.get_name())
                self.exit_code = os.EX_SOFTWARE
                raise dl_app_base.UnexpectedFinishError
            elif task.cancelled():
                LOGGER.error("Task %s was cancelled", task.get_name())
                self.exit_code = os.EX_SOFTWARE
                raise exception
            else:
                LOGGER.error("Task %s finished with exception", task.get_name())
                self.exit_code = os.EX_SOFTWARE
                raise dl_app_base.RunError from exception

    async def _cleanup(self) -> None:
        await self._cleanup_non_aiohttp_main_tasks()
        await self._cleanup_application()
        await self._cleanup_aiohttp_server()

    async def _cleanup_non_aiohttp_main_tasks(self) -> None:
        if not self._non_aiohttp_main_tasks:
            return

        LOGGER.info("Cancelling non-aiohttp main tasks...")
        finished_unexpectedly: list[asyncio.Task[None]] = []
        finished_with_exception: list[asyncio.Task[None]] = []
        unfinished_tasks: list[asyncio.Task[None]] = []
        cancelled_tasks: list[asyncio.Task[None]] = []

        for task in self._non_aiohttp_main_tasks:
            if not task.done():
                unfinished_tasks.append(task)
            elif task.cancelled():
                cancelled_tasks.append(task)
            elif task.exception() is not None:
                finished_with_exception.append(task)
            else:
                finished_unexpectedly.append(task)
        if finished_unexpectedly:
            LOGGER.info("Tasks that finished unexpectedly:")
            for task in finished_unexpectedly:
                LOGGER.info("- %s", task.get_name())

        if finished_with_exception:
            LOGGER.info("Tasks that finished with exception:")
            for task in finished_with_exception:
                LOGGER.info("- %s", task.get_name())

        if cancelled_tasks:
            LOGGER.info("Tasks that were cancelled:")
            for task in cancelled_tasks:
                LOGGER.info("- %s", task.get_name())

        if unfinished_tasks:
            LOGGER.info("Unfinished tasks:")
            for task in unfinished_tasks:
                LOGGER.info("- %s - cancelling...", task.get_name())
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    LOGGER.info("- %s - cancelled", task.get_name())

    async def _cleanup_application(self) -> None:
        if not self._application:
            return

        try:
            await self._application.on_shutdown()
        except dl_app_base.ShutdownError:
            LOGGER.exception("Failed to shutdown application")
            self.exit_code = os.EX_SOFTWARE

    async def _cleanup_aiohttp_server(self) -> None:
        if not self._runner:
            return

        try:
            await self._runner.cleanup()
        except Exception:
            LOGGER.exception("Failed to cleanup runner")
            self.exit_code = os.EX_SOFTWARE
