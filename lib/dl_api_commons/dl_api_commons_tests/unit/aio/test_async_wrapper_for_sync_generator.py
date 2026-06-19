import asyncio
from collections.abc import (
    AsyncGenerator,
    Callable,
    Generator,
)
from concurrent.futures.thread import ThreadPoolExecutor
import logging
import random
import threading
import time
from typing import Any

import attr
import pytest

from dl_api_commons.aio.async_wrapper_for_sync_generator import (
    EndOfStreamError,
    InitializationFailedError,
    Job,
    JobState,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def service_tpe() -> Generator[ThreadPoolExecutor, None, None]:
    tpe = ThreadPoolExecutor(thread_name_prefix="SERVICE_TPE_")
    yield tpe
    tpe.shutdown()


@pytest.fixture
def worker_tpe() -> Generator[ThreadPoolExecutor, None, None]:
    tpe = ThreadPoolExecutor(thread_name_prefix="WORKER_TPE_")
    yield tpe
    tpe.shutdown()


@pytest.fixture
def wrapper_factory(
    worker_tpe: ThreadPoolExecutor,
    service_tpe: ThreadPoolExecutor,
) -> Callable[[Callable[[], Generator[Any, None, None]]], Job]:
    @attr.s(auto_attribs=True, kw_only=True)
    class MyGenerator(Job):
        _generator: Callable[[], Generator[Any, None, None]]

        def make_generator(self) -> Generator[Any, None, None]:
            return self._generator()

    def wrapper(generator: Callable[[], Generator[Any, None, None]]) -> Job:
        return MyGenerator(
            generator=generator,
            service_tpe=service_tpe,
            workers_tpe=worker_tpe,
        )

    return wrapper


async def async_gen_adapter(wr: Job) -> AsyncGenerator[int, None]:
    await wr.run()

    while True:
        try:
            yield await wr.get_next()
        except EndOfStreamError:
            return


@pytest.mark.asyncio
async def test_simple_finite_generator(
    wrapper_factory: Callable[[Callable[[], Generator[Any, None, None]]], Job],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")
    count = int(1e3)

    def test_generator() -> Generator[Any, None, None]:
        yield from range(count)

    job = wrapper_factory(test_generator)

    result = [item async for item in async_gen_adapter(job)]

    assert result == list(range(count))
    assert job.state == JobState.closed
    await job.cancel()


@pytest.mark.asyncio
async def test_error_in_start_generator(
    wrapper_factory: Callable[[Callable[[], Generator[Any, None, None]]], Job],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")

    def test_generator() -> Generator[int, None, None]:
        raise ValueError("Error in generator")
        yield

    job = wrapper_factory(test_generator)
    await job.run()

    with pytest.raises(ValueError, match=r"^Error in generator$"):
        await job.get_next()

    assert job.state == JobState.closed


@pytest.mark.asyncio
async def test_error_in_generator_creation(
    wrapper_factory: Callable[[Callable[[], Generator[Any, None, None]]], Job],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")

    class MarkerError(Exception):
        pass

    def broken_generator_factory() -> Generator[int, None, None]:
        raise MarkerError

    job = wrapper_factory(broken_generator_factory)

    with pytest.raises(MarkerError):
        await job.run()


@pytest.mark.asyncio
async def test_start_confirmation_timeout(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")

    local_worker_tpe = ThreadPoolExecutor(max_workers=1)
    local_service_tpe = ThreadPoolExecutor(max_workers=10)

    class LocalJob(Job):
        def make_generator(self) -> Generator[Any, None, None]:
            def g() -> Generator[Any, None, None]:
                yield None

            return g()

    loop = asyncio.get_running_loop()
    garbage_started = asyncio.Event()
    stop_garbage = threading.Event()

    def garbage() -> None:
        LOGGER.info("Garbage thread was started")

        def cb() -> None:
            LOGGER.info("Triggering event 'garbage_started'")
            garbage_started.set()

        loop.call_soon_threadsafe(cb)
        got_stop_event = stop_garbage.wait(timeout=15)
        if got_stop_event:
            LOGGER.info("Stopping garbage thread due to stop event")
        else:
            LOGGER.info("Stopping garbage thread due to timeout")
        return

    try:
        # `garbage` is intentionally left dangling — the test exercises GC of an orphaned task.
        asyncio.ensure_future(asyncio.get_running_loop().run_in_executor(local_worker_tpe, garbage))  # noqa: RUF006
        await asyncio.wait_for(garbage_started.wait(), timeout=1)

        job = LocalJob(
            service_tpe=local_service_tpe,
            workers_tpe=local_worker_tpe,
        )

        with pytest.raises(InitializationFailedError):
            await job.run()

        await job.cancel()

    finally:
        LOGGER.info("Executing test clean-up")
        # Terminating garbage thread
        stop_garbage.set()
        local_worker_tpe.shutdown(wait=True)
        local_service_tpe.shutdown(wait=True)
        LOGGER.info("Test clean-up done")


@pytest.mark.asyncio
async def test_closing_before_full_consuming(
    wrapper_factory: Callable[[Callable[[], Generator[Any, None, None]]], Job],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")

    def test_generator() -> Generator[Any, None, None]:
        try:
            while True:
                time.sleep(0.5)
                yield random.randint(0, int(1e6))
        finally:
            LOGGER.info("Generator was correctly closed")

    job = wrapper_factory(test_generator)
    await job.run()

    await job.get_next()
    await job.get_next()

    await job.cancel()

    assert job.state == JobState.closed
    assert "Generator was correctly closed" in caplog.messages
