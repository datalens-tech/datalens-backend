import asyncio

import pytest

from dl_utils.aio import (
    async_run,
    await_sync,
)


@pytest.mark.filterwarnings("ignore")
def test_await_sync():
    async def async_func():
        return 42

    assert await_sync(async_func()) == 42


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore")
async def test_await_sync_from_sync_context_raises():
    async def async_func():
        return 42

    def sync_context():
        return await_sync(async_func())

    async def async_context():
        return sync_context()

    with pytest.raises(RuntimeError):
        await async_context()


def test_async_run():
    async def async_func():
        return 42

    assert async_run(async_func()) == 42


def test_async_run_no_current_loop():
    async def async_func():
        return 42

    def sync_context():
        return async_run(async_func())

    result = sync_context()
    assert result == 42


@pytest.mark.asyncio
async def test_async_run_not_disturbs_running_loop():
    async def async_func():
        return 42

    def sync_context():
        return async_run(async_func())

    loop = asyncio.get_running_loop()
    assert loop.is_running()

    result = sync_context()
    assert result == 42

    assert asyncio.get_running_loop() == loop
    assert loop.is_running()
