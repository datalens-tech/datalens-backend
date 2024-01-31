from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from enum import IntEnum, unique
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr

LOGGER = logging.getLogger(__name__)


@unique
class CMState(IntEnum):
    initialized = 1
    entered = 2
    exiting = 3
    exited = 4


_PE_RET_TV = TypeVar("_PE_RET_TV")


class PreExitable:
    """An AsyncContextManager wrapper that allows exiting before the `with` block is over"""

    def __init__(self, cm: AsyncContextManager[_PE_RET_TV]) -> None:
        self._cm = cm
        self._state = CMState.initialized
        self._exiting_task: Optional[asyncio.Future] = None

    async def __aenter__(self) -> _PE_RET_TV:
        assert self._state == CMState.initialized
        self._state = CMState.entered
        return await self._cm.__aenter__()

    async def _call_aexit(self, *exc_details: Any) -> Any:
        try:
            await self._cm.__aexit__(*exc_details)
        finally:
            self._state = CMState.exited
            self._exiting_task = None

    async def __aexit__(self, *exc_details: Any) -> Any:
        if self._state == CMState.exited:
            return None
        if self._state == CMState.exiting:
            # This makes it possible to close multiple times, with all calls
            # waiting on the finalization.
            assert self._exiting_task is not None
            return await asyncio.shield(self._exiting_task)
        assert self._state == CMState.entered
        self._state = CMState.exiting
        exiting_task = asyncio.ensure_future(
            asyncio.shield(self._call_aexit(*exc_details))
        )
        self._exiting_task = exiting_task
        return await exiting_task

    async def exit(self) -> Any:
        return await self.__aexit__(None, None, None)


@contextlib.asynccontextmanager
async def task_cm(coro: Awaitable) -> AsyncGenerator[asyncio.Task, None]:
    """Small helper to run an asyncio task for the duration of the context manager"""
    # While it doesn't actually do any `await`, it would be weird to expect non-async here.
    task = asyncio.create_task(coro)
    try:
        yield task
    finally:
        task.cancel()
        # Ensure the cancelling is synchronous:
        try:
            await task
        except asyncio.CancelledError:
            pass


@contextlib.asynccontextmanager
async def await_on_exit(coro: Awaitable) -> AsyncGenerator[Any, None]:
    try:
        yield coro
    finally:
        await coro


@attr.s(auto_attribs=True)
class CacheShareItem:
    key: str
    done: bool = attr.ib(default=False)
    result: Any = attr.ib(default=None)
    lock: asyncio.Lock = attr.ib(factory=asyncio.Lock)
    waiters: Set[Any] = attr.ib(factory=set)


def get_current_task_name() -> Optional[str]:
    current_task = asyncio.current_task()
    if current_task is not None:
        get_name = getattr(current_task, "get_name", None)  # python 3.7 doesn't
        if get_name is not None and callable(get_name):
            return get_name()
    return None


def new_self_id() -> str:
    pieces = [
        "h_" + socket.gethostname(),
        "p_" + str(os.getpid()),
    ]
    thread_name = threading.current_thread().name
    if thread_name and thread_name != "MainThread":
        pieces.append("t_" + thread_name)  # rare
    current_task_name = get_current_task_name()
    if current_task_name:
        pieces.append("a_" + current_task_name)
    pieces.append("r_" + str(uuid.uuid4()))
    return "_".join(pieces)


_CSS_RES_TV = TypeVar("_CSS_RES_TV")


@attr.s
class CacheShareSingleton:
    """
    A helper for in-memory synchronization of cached value generation.

    Intended to be used as a wrapper around `RedisCacheLock` calls, for a bit
    more performance.
    """

    item_cls: Type[CacheShareItem] = CacheShareItem

    cache: Dict[str, CacheShareItem] = attr.ib(factory=dict)
    track_waiters: bool = attr.ib(default=True)
    debug: bool = attr.ib(default=False)

    def _debug(self, msg: str, *args: Any) -> None:
        if self.debug:
            LOGGER.debug(msg, *args)

    async def generate_with_cache(
        self,
        key: str,
        generate: Callable[[], Awaitable[_CSS_RES_TV]],
    ) -> _CSS_RES_TV:
        cache_item = self.cache.get(key)
        if cache_item is None:
            self._debug("Initializing: key=%r", key)
            cache_item = self.item_cls(key=key)
            self.cache[key] = cache_item

        # Not a normal case, actually:
        # should imply non-empty `cache_item.waiters`
        if cache_item.done:
            self._debug("Found ready: key=%r, item=%r", key, cache_item)  # rare
            return cache_item.result  # rare

        this_request = (object(), get_current_task_name() or str(uuid.uuid4()))
        assert this_request not in cache_item.waiters

        try:
            # This can be removed in favor of python's refcounting (using weakref in `self.cache`),
            # for more performance and less debuggability.
            cache_item.waiters.add(this_request)

            self._debug("Locking %r...", key)
            async with cache_item.lock:
                if cache_item.done:
                    self._debug("Locked %r, found it done.", key)
                    return cache_item.result

                self._debug("Generating %r...", key)
                result = await generate()
                cache_item.result = result
                cache_item.done = True
                return result

        finally:
            cache_item.waiters.remove(this_request)
            if not cache_item.waiters:
                self._debug("Clearing %r.", key)
                # Make it garbage-collectable soon enough:
                self.cache.pop(key)


_GF_RET_TV = TypeVar("_GF_RET_TV")


def wrap_generate_func(
    func: Callable[[], Awaitable[_GF_RET_TV]],
    serialize: Callable[[Any], Union[bytes, str]] = json.dumps,
    default_encoding: str = "utf-8",
) -> Callable[[], Awaitable[Tuple[bytes, _GF_RET_TV]]]:
    """
    Given a function that returns some value, wrap it to also return the
    serialized version of the value for use in RedisCacheLock.
    """

    async def wrapped_generate_func() -> Tuple[bytes, _GF_RET_TV]:
        result = await func()
        result_b = serialize(result)
        if isinstance(result_b, str):
            result_b = result_b.encode(default_encoding)
        return result_b, result

    return wrapped_generate_func


# This makes `time.time_ns() - time.monotonic_ns()` but with slightly more precision.
_T1 = time.monotonic_ns()
_T2 = time.time_ns()
_T3 = time.monotonic_ns()
MONOTIME_NS_OFFSET = _T2 - _T1 // 2 - _T3 // 2
MONOTIME_OFFSET = MONOTIME_NS_OFFSET / 1e9


def monotime_ns() -> int:
    """
    Monotonic nanoseconds time that approximates unix nanosecond timestamp,
    for precision and convenience.
    """
    return time.monotonic_ns() + MONOTIME_NS_OFFSET


def monotime() -> float:
    """
    Monotonic time that approximates unix timestamp,
    for maximum convenience.
    """
    return time.monotonic() + MONOTIME_OFFSET


@attr.s(auto_attribs=True)
class HistoryHolder:
    """
    An in-memory holder that can be used as `debug_log` callable
    (optionally wrapping another callable)
    """

    max_size: int = 10_000
    func: Optional[Callable[[str, Dict[str, Any]], None]] = None

    history: List[Tuple[float, str, Dict[str, Any]]] = attr.ib(factory=list, repr=False)

    def __call__(self, msg: str, details: Dict[str, Any]) -> None:
        if len(self.history) >= self.max_size:
            self.history.pop(0)
        self.history.append((monotime(), msg, details))
        if self.func is not None:
            self.func(msg, details)  # pylint: disable=not-callable

    def __iter__(self) -> Iterable[Tuple[float, str, Dict[str, Any]]]:
        return iter(list(self.history))
