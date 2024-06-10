import asyncio
import logging
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Union,
)


LOGGER = logging.getLogger(__name__)


def wait_for(
    name: str,
    condition: Callable[[], Union[bool, Tuple[bool, str]]],
    timeout: float = 300.0,
    interval: float = 1.0,
    log_func: Optional[Callable[[str], None]] = None,
    require: bool = True,
) -> Tuple[bool, str]:
    """
    `condition` should either return `is_done` or `(is_done, status)`.
    """
    start_time = time.monotonic()
    max_time = start_time + timeout

    status: str
    while True:
        loop_start_time = time.monotonic()

        cond_result = condition()
        if isinstance(cond_result, bool):
            cond_ok = cond_result
            status = ""
        else:
            cond_ok, status = cond_result

        if cond_ok:
            return True, status

        now = time.monotonic()
        if now > max_time:
            message = f"Timed out waiting for {name!r}, last status {status!r}, timeout {timeout:.3f}s."
            if require:
                raise TimeoutError(message)
            LOGGER.error(message)
            return False, status

        next_try_time = loop_start_time + interval
        sleep_time = next_try_time - now
        if log_func is not None:
            log_func(f"Waiting for {name!r} (sleep {sleep_time:.3f}s, status {status!r}).")
        if sleep_time > 0.001:
            time.sleep(sleep_time)


async def await_for(
    name: str,
    condition: Callable[[], Awaitable[Union[bool, Tuple[bool, Any]]]],
    timeout: float = 300.0,
    interval: float = 1.0,
    log_func: Optional[Callable[[str], None]] = None,
    require: bool = True,
) -> Tuple[bool, str]:
    """
    `condition` should either return `is_done` or `(is_done, status)`.
    """
    start_time = time.monotonic()
    max_time = start_time + timeout

    status: str
    while True:
        loop_start_time = time.monotonic()

        cond_result = await condition()
        if isinstance(cond_result, bool):
            cond_ok = cond_result
            status = ""
        else:
            cond_ok, status = cond_result

        if cond_ok:
            return True, status

        now = time.monotonic()
        if now > max_time:
            message = f"Timed out waiting for {name!r}, status {status!r}, timeout {timeout:.3f}s."
            if require:
                raise TimeoutError(message)
            LOGGER.error(message)
            return False, status

        next_try_time = loop_start_time + interval
        sleep_time = next_try_time - now
        if log_func is not None:
            remaining = max_time - time.monotonic()
            log_func(f"Waiting for {name!r} (sleep {sleep_time:.3f}s, limit {remaining:.3f}s, status {status!r}).")
        if sleep_time > 0.001:
            await asyncio.sleep(sleep_time)
