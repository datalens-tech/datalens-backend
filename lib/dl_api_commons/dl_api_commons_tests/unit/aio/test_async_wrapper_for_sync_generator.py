from __future__ import annotations

import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
import inspect
import logging
import random
import threading
import time
from typing import (
    Any,
    Callable,
    Generator,
    Union,
)

import attr
import pytest

from dl_api_commons.aio.async_wrapper_for_sync_generator import (
    EndOfStream,
    InitializationFailed,
    Job,
    JobState,
)


@pytest.fixture()
def service_tpe() -> ThreadPoolExecutor:
    tpe = ThreadPoolExecutor(thread_name_prefix="SERVICE_TPE_")
    yield tpe
    tpe.shutdown()


@pytest.fixture()
def worker_tpe() -> ThreadPoolExecutor:
    tpe = ThreadPoolExecutor(thread_name_prefix="WORKER_TPE_")
    yield tpe
    tpe.shutdown()


@pytest.fixture()
def wrapper_factory(worker_tpe, service_tpe) -> Callable[[Generator], Job]:
    @attr.s
    class MyGenerator(Job):
        _generator: Union[Generator, Callable[[], Generator]] = attr.ib(default=None)

        def make_generator(self) -> Generator[Any, bool, None]:
            if inspect.isgenerator(self._generator):
                return self._generator
            else:
                return self._generator()

    return lambda gen: MyGenerator(
        service_tpe=service_tpe,
        workers_tpe=worker_tpe,
        generator=gen,
    )


async def async_gen_adapter(wr: Job):
    await wr.run()

    while True:
        try:
            yield await wr.get_next()
        except EndOfStream:
            return


@pytest.mark.asyncio
async def test_simple_finite_generator(wrapper_factory, caplog):
    caplog.set_level("DEBUG")
    count = int(1e3)

    def test_generator():
        for i in range(count):
            yield i

    job = wrapper_factory(test_generator())

    result = []
    async for item in async_gen_adapter(job):
        result.append(item)

    assert result == list(range(count))
    assert job.state == JobState.closed
    await job.cancel()


@pytest.mark.asyncio
async def test_error_in_start_generator(wrapper_factory, caplog):
    caplog.set_level("DEBUG")

    def test_generator():
        raise ValueError("Error in generator")
        # noqa
        yield

    job = wrapper_factory(test_generator())
    await job.run()

    with pytest.raises(ValueError, match=r"^Error in generator$"):
        await job.get_next()

    assert job.state == JobState.closed


@pytest.mark.asyncio
async def test_error_in_generator_creation(wrapper_factory, caplog):
    caplog.set_level("DEBUG")

    class MarkerException(Exception):
        pass

    def broken_generator_factory():
        raise MarkerException

    job = wrapper_factory(lambda: broken_generator_factory())

    with pytest.raises(MarkerException):
        await job.run()


@pytest.mark.asyncio
async def test_start_confirmation_timeout(caplog):
    caplog.set_level("DEBUG")

    local_worker_tpe = ThreadPoolExecutor(max_workers=1)
    local_service_tpe = ThreadPoolExecutor(max_workers=10)

    class LocalJob(Job):
        def make_generator(self) -> Generator[Any, None, None]:
            def g():
                yield None

            return g()

    loop = asyncio.get_running_loop()
    garbage_started = asyncio.Event()
    stop_garbage = threading.Event()

    def garbage():
        logging.info("Garbage thread was started")

        def cb():
            logging.info("Triggering event 'garbage_started'")
            garbage_started.set()

        loop.call_soon_threadsafe(cb)
        got_stop_event = stop_garbage.wait(timeout=15)
        if got_stop_event:
            logging.info("Stopping garbage thread due to stop event")
        else:
            logging.info("Stopping garbage thread due to timeout")
        return

    try:
        asyncio.ensure_future(asyncio.get_running_loop().run_in_executor(local_worker_tpe, garbage))
        await asyncio.wait_for(garbage_started.wait(), timeout=1)

        job = LocalJob(
            service_tpe=local_service_tpe,
            workers_tpe=local_worker_tpe,
        )

        with pytest.raises(InitializationFailed):
            await job.run()

        await job.cancel()

    finally:
        logging.info("Executing test clean-up")
        # Terminating garbage thread
        stop_garbage.set()
        local_worker_tpe.shutdown(wait=True)
        local_service_tpe.shutdown(wait=True)
        logging.info("Test clean-up done")


@pytest.mark.asyncio
async def test_closing_before_full_consuming(wrapper_factory, caplog):
    caplog.set_level("INFO")

    def test_generator():
        try:
            while True:
                time.sleep(0.5)
                yield random.randint(0, int(1e6))
        finally:
            logging.info("Generator was correctly closed")

    job = wrapper_factory(test_generator())
    await job.run()

    await job.get_next()
    await job.get_next()

    await job.cancel()

    assert job.state == JobState.closed
    assert "Generator was correctly closed" in caplog.messages
