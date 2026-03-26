import pydantic
import pytest

import dl_httpx


def _max_parallel_limiter(max_parallel: int) -> dl_httpx.MaxParallelRateLimiter:
    settings = dl_httpx.RateLimiterSettings.factory(
        {"TYPE": "MAX_PARALLEL", "MAX_PARALLEL": max_parallel},
    )
    assert isinstance(settings, dl_httpx.MaxParallelRateLimiterSettings)
    return dl_httpx.MaxParallelRateLimiter.from_settings(settings)


@pytest.mark.parametrize("value", (0, -1))
def test_max_parallel_invalid(value: int) -> None:
    with pytest.raises(pydantic.ValidationError):
        dl_httpx.RateLimiterSettings.factory(
            {"TYPE": "MAX_PARALLEL", "MAX_PARALLEL": value},
        )


def test_max_parallel_one_nested_second_raises_sync() -> None:
    limiter = _max_parallel_limiter(1)
    with limiter.context():
        with pytest.raises(dl_httpx.RateLimitHttpxClientException):
            with limiter.context():
                pass


@pytest.mark.asyncio
async def test_max_parallel_one_nested_second_raises_async() -> None:
    limiter = _max_parallel_limiter(1)
    async with limiter.context_async():
        with pytest.raises(dl_httpx.RateLimitHttpxClientException):
            async with limiter.context_async():
                pass


def test_max_parallel_allows_nested_up_to_limit_sync() -> None:
    limiter = _max_parallel_limiter(2)
    with limiter.context():
        with limiter.context():
            pass


@pytest.mark.asyncio
async def test_max_parallel_allows_nested_up_to_limit_async() -> None:
    limiter = _max_parallel_limiter(2)
    async with limiter.context_async():
        async with limiter.context_async():
            pass


def test_max_parallel_at_capacity_raises_sync() -> None:
    limiter = _max_parallel_limiter(2)
    with limiter.context():
        with limiter.context():
            with pytest.raises(dl_httpx.RateLimitHttpxClientException):
                with limiter.context():
                    pass


@pytest.mark.asyncio
async def test_max_parallel_at_capacity_raises_async() -> None:
    limiter = _max_parallel_limiter(2)
    async with limiter.context_async():
        async with limiter.context_async():
            with pytest.raises(dl_httpx.RateLimitHttpxClientException):
                async with limiter.context_async():
                    pass


def test_max_parallel_release_after_exit_allows_next_sync() -> None:
    limiter = _max_parallel_limiter(1)
    with limiter.context():
        pass
    with limiter.context():
        pass


@pytest.mark.asyncio
async def test_max_parallel_release_after_exit_allows_next_async() -> None:
    limiter = _max_parallel_limiter(1)
    async with limiter.context_async():
        pass
    async with limiter.context_async():
        pass


def test_max_parallel_release_on_exception_sync() -> None:
    limiter = _max_parallel_limiter(1)
    with pytest.raises(RuntimeError, match="fail"):
        with limiter.context():
            raise RuntimeError("fail")
    with limiter.context():
        pass


@pytest.mark.asyncio
async def test_max_parallel_release_on_exception_async() -> None:
    limiter = _max_parallel_limiter(1)
    with pytest.raises(RuntimeError, match="fail"):
        async with limiter.context_async():
            raise RuntimeError("fail")
    async with limiter.context_async():
        pass
