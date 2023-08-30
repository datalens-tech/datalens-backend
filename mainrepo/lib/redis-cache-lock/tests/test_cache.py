# pylint: disable=redefined-outer-name
from __future__ import annotations

import asyncio
import itertools
import logging
import uuid
from typing import TYPE_CHECKING, AsyncGenerator, Callable

import pytest

from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.redis_utils import make_simple_cli_acm
from redis_cache_lock.utils import HistoryHolder, wrap_generate_func

if TYPE_CHECKING:
    from redis_cache_lock.types import TClientACM


@pytest.fixture
async def cli_acm() -> TClientACM:
    return make_simple_cli_acm("redis://localhost")


def lock_mgr(cli_acm, **kwargs) -> RedisCacheLock:
    def log_func(msg, details):
        logging.debug(
            "RedisCacheLock: %s, %s",
            msg, ", ".join(f"{key}={val!r}" for key, val in details.items()))

    logs = HistoryHolder(func=log_func)
    tasks = []

    def task_cb(task: asyncio.Task) -> None:
        tasks.append(task)

    full_kwargs = dict(
        key="",
        client_acm=cli_acm,
        resource_tag="tst",
        lock_ttl_sec=2.3,
        data_ttl_sec=5.6,
        debug_log=logs,
        bg_task_callback=task_cb,
    )
    full_kwargs.update(kwargs)
    mgr = RedisCacheLock(**full_kwargs)
    setattr(mgr, "logs_", logs)
    setattr(mgr, "tasks_", tasks)
    return mgr


@pytest.fixture(params=["fg", "bg"])
def backgroundness_mode(request) -> bool:
    return request.param == "bg"


@pytest.fixture
async def lock_mgr_gen(
    cli_acm,
    backgroundness_mode,
) -> AsyncGenerator[Callable[[], RedisCacheLock], None]:
    lock_mgrs = []

    def lock_mgr_gen_func(*args, **kwargs):
        kwargs = {
            "cli_acm": cli_acm,
            "enable_background_tasks": backgroundness_mode,
            **kwargs,
        }
        mgr = lock_mgr(*args, **kwargs)
        lock_mgrs.append(mgr)
        return mgr

    try:
        yield lock_mgr_gen_func
    finally:
        tasks = [task for mgr in lock_mgrs for task in getattr(mgr, "tasks_")]
        await asyncio.gather(*tasks, return_exceptions=True)


class CounterGenerate:
    def __init__(self, sleep_time: float = 0.01):
        self.sleep_time = sleep_time
        self.cnt = itertools.count(1)

    async def _raw_generate_func(self):
        await asyncio.sleep(self.sleep_time)
        return dict(value=next(self.cnt))

    @property
    def generate_func(self):
        return wrap_generate_func(self._raw_generate_func)


@pytest.mark.asyncio
async def test_minimal_lock(lock_mgr_gen):
    count = 5
    key = str(uuid.uuid4())

    lock_mgr = lock_mgr_gen(key=key)
    gen = CounterGenerate()

    for idx in range(count):
        result_b, result_raw = await lock_mgr.generate_with_lock(gen.generate_func)
        if idx == 0:
            assert result_raw == dict(value=1)
        else:
            assert result_raw is None
        assert result_b == b'{"value": 1}', idx

    logs: HistoryHolder = getattr(lock_mgr, "logs_")
    assert logs.history[-1][-1]["situation"] == lock_mgr.req_situation.cache_hit_slave

    result_b, result_raw = await lock_mgr.clone(key=key + "02").generate_with_lock(
        generate_func=gen.generate_func,
    )
    assert result_raw == dict(value=2)
    assert result_b == b'{"value": 2}'


@pytest.mark.asyncio
async def test_sync_lock(lock_mgr_gen):
    count = 7
    key = str(uuid.uuid4())

    mgrs = [lock_mgr_gen(key=key) for _ in range(count)]
    gen = CounterGenerate()

    results = await asyncio.gather(
        *[
            lock_mgr.generate_with_lock(generate_func=gen.generate_func)
            for lock_mgr in mgrs
        ],
    )

    assert results
    item_full = (b'{"value": 1}', {"value": 1})
    item_cached = (b'{"value": 1}', None)
    res_full = [item for item in results if item == item_full]
    res_cached = [item for item in results if item == item_cached]
    assert len(res_full) == 1, results
    assert len(res_cached) == count - 1, results


@pytest.mark.asyncio
async def test_sync_long_lock(lock_mgr_gen):
    count = 7
    key = str(uuid.uuid4())

    mgrs = [lock_mgr_gen(key=key) for _ in range(count)]
    gen = CounterGenerate(sleep_time=5)

    results = await asyncio.gather(
        *[
            lock_mgr.generate_with_lock(generate_func=gen.generate_func)
            for lock_mgr in mgrs
        ],
    )

    assert results
    item_full = (b'{"value": 1}', {"value": 1})
    item_cached = (b'{"value": 1}', None)
    res_full = [item for item in results if item == item_full]
    res_cached = [item for item in results if item == item_cached]
    assert len(res_full) == 1, results
    assert len(res_cached) == count - 1, results
