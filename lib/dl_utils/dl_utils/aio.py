from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextvars
from functools import partial
import logging
import threading
from types import TracebackType
import typing
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)
import weakref

from async_timeout import Timeout as VanillaTimeout


if TYPE_CHECKING:
    from concurrent.futures import Future


LOGGER = logging.getLogger(__name__)

_CORO_TV = TypeVar("_CORO_TV")


async def shield_wait_for_complete(
    coro: Awaitable[_CORO_TV], log_msg_on_cancel: str = "CancelledError during critical section execution"
) -> _CORO_TV:
    """
    Shields coroutine from cancellation and waits until it finished.
    If execution was cancelled - raise CancelledError after coroutine done.
    Logs exceptions if task was cancelled and exception occurs.
    :param coro: Coroutine to shield
    :param log_msg_on_cancel: message to log in case of cancellation (will be logged exactly when cancellation occurs)
    :return: shielded coroutine result
    """
    loop = asyncio.get_event_loop()
    real_task = loop.create_task(coro)  # type: ignore  # 2024-01-24 # TODO: Need type annotation for "real_task"  [var-annotated]

    try:
        return await asyncio.shield(real_task)
    except asyncio.CancelledError:
        LOGGER.info(log_msg_on_cancel)

        # Awaiting protected task and logging exception if occurred
        try:
            await real_task
        except Exception as e:  # noqa
            logging.exception("Error during awaiting critical section after cancellation")

        # Raising original exception to meet
        raise


_SUBMIT_RT = TypeVar("_SUBMIT_RT")


class ContextVarExecutor(ThreadPoolExecutor):
    """Bug in Python: default TPE does not propagate ContextVars in running thread."""

    def __init__(self, *args, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        super().__init__(*args, **kwargs)
        self._futures_set = weakref.WeakSet()  # type: ignore  # 2024-01-30 # TODO: Need type annotation for "_futures_set"  [var-annotated]

    def get_pending_futures_count(self) -> int:
        return len([fut for fut in self._futures_set if fut.running()])

    def submit(self, fn: Callable[..., _SUBMIT_RT], /, *args: Any, **kwargs: Any) -> Future[_SUBMIT_RT]:
        ctx = contextvars.copy_context()  # type: contextvars.Context
        fut = super().submit(cast(Callable[..., _SUBMIT_RT], partial(ctx.run, partial(fn, *args, **kwargs))))
        # TODO FIX: Periodically cleanup set
        self._futures_set.add(fut)
        return fut


_WRAPPED_RT = TypeVar("_WRAPPED_RT")


def await_sync(coro: Awaitable[_WRAPPED_RT], loop: Optional[asyncio.AbstractEventLoop] = None) -> _WRAPPED_RT:
    if loop is None:
        loop = asyncio.get_event_loop()

    return loop.run_until_complete(coro)


class RunThread(threading.Thread):
    def __init__(self, coro: typing.Coroutine[typing.Any, typing.Any, _WRAPPED_RT]):
        self.coro = coro
        self.result: _WRAPPED_RT | None = None
        super().__init__()

    def run(self) -> None:
        self.result = asyncio.run(self.coro)


# Threading version of await_sync, needed to be able to run async code in sync code, which is running in async context.
# This is needed due to constant mixing of context in the codebase.
# Should be avoided by making context async and using await_sync on higher level of sync counterparts.
# USE IT ONLY IN THE RAREST OF OCCASIONS, WHEN YOU HAVE NO OTHER CHOICE.
# Please, lord, have mercy on my soul, for I have sinned.
def async_run(
    coro: typing.Coroutine[typing.Any, typing.Any, _WRAPPED_RT],
    loop: typing.Optional[asyncio.AbstractEventLoop] = None,
) -> _WRAPPED_RT:
    if loop is None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None

    if loop is None:
        return asyncio.run(coro)

    if not loop.is_running():
        return loop.run_until_complete(coro)

    thread = RunThread(coro)
    thread.start()
    thread.join()

    assert thread.result is not None
    return thread.result


_ITERABLE_T = TypeVar("_ITERABLE_T")


def to_sync_iterable(
    async_iterable: AsyncIterable[_ITERABLE_T],
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> Iterable[_ITERABLE_T]:
    actual_loop = asyncio.get_event_loop() if loop is None else loop

    def wrapper() -> Iterable[_ITERABLE_T]:
        aiter = async_iterable.__aiter__()
        while True:
            try:
                yield actual_loop.run_until_complete(aiter.__anext__())
            except StopAsyncIteration:
                return

    return wrapper()


async def alist(aiterable: AsyncIterable[_ITERABLE_T]) -> List[_ITERABLE_T]:
    """Gather an async iterable into a single list"""
    result = []
    async for item in aiterable:
        result.append(item)
    return result


class BIAsyncTimeout:
    vanilla_timeout: VanillaTimeout

    @classmethod
    def from_vanilla(cls, vanilla_timeout: VanillaTimeout) -> BIAsyncTimeout:
        instance = cls()
        instance.vanilla_timeout = vanilla_timeout
        return instance

    @classmethod
    def from_params(cls, deadline: Optional[float], loop: asyncio.AbstractEventLoop) -> BIAsyncTimeout:
        return cls.from_vanilla(VanillaTimeout(deadline, loop))

    async def __aenter__(self) -> BIAsyncTimeout:
        return BIAsyncTimeout.from_vanilla(await self.vanilla_timeout.__aenter__())

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        return await self.vanilla_timeout.__aexit__(exc_type, exc_val, exc_tb)


def timeout(delay: Optional[float]) -> BIAsyncTimeout:
    loop = asyncio.get_running_loop()
    if delay is not None:
        deadline = loop.time() + delay  # type: Optional[float]
    else:
        deadline = None
    return BIAsyncTimeout.from_params(deadline, loop)
