import unittest.mock

import pydantic
import pytest

import dl_httpx


@pytest.mark.parametrize("value", (0, -1))
def test_sliding_window_invalid_max_requests(value: int) -> None:
    with pytest.raises(pydantic.ValidationError):
        dl_httpx.RateLimiterSettings.factory(
            {
                "TYPE": "SLIDING_WINDOW",
                "MAX_REQUESTS": value,
                "WINDOW_SECONDS": 1.0,
            },
        )


@pytest.mark.parametrize("value", (0, -1.0))
def test_sliding_window_invalid_window(value: float) -> None:
    with pytest.raises(pydantic.ValidationError):
        dl_httpx.RateLimiterSettings.factory(
            {
                "TYPE": "SLIDING_WINDOW",
                "MAX_REQUESTS": 1,
                "WINDOW_SECONDS": value,
            },
        )


def test_sliding_window_full_sync() -> None:
    settings = dl_httpx.RateLimiterSettings.factory(
        {
            "TYPE": "SLIDING_WINDOW",
            "MAX_REQUESTS": 2,
            "WINDOW_SECONDS": 60.0,
        },
    )
    assert isinstance(settings, dl_httpx.SlidingWindowRateLimiterSettings)
    limiter = dl_httpx.SlidingWindowRateLimiter.from_settings(settings)
    with limiter.context():
        pass
    with limiter.context():
        pass
    with pytest.raises(dl_httpx.RateLimitHttpxClientException):
        with limiter.context():
            pass


@pytest.mark.asyncio
async def test_sliding_window_full_async() -> None:
    settings = dl_httpx.RateLimiterSettings.factory(
        {
            "TYPE": "SLIDING_WINDOW",
            "MAX_REQUESTS": 2,
            "WINDOW_SECONDS": 60.0,
        },
    )
    assert isinstance(settings, dl_httpx.SlidingWindowRateLimiterSettings)
    limiter = dl_httpx.SlidingWindowRateLimiter.from_settings(settings)
    async with limiter.context_async():
        pass
    async with limiter.context_async():
        pass
    with pytest.raises(dl_httpx.RateLimitHttpxClientException):
        async with limiter.context_async():
            pass


def test_sliding_window_passes_after_window_elapses() -> None:
    datetime_provider = unittest.mock.Mock(spec=dl_httpx.DateTimeProvider)
    datetime_provider.get_now.side_effect = [0.0, 15.0]
    limiter = dl_httpx.SlidingWindowRateLimiter(
        max_requests=1,
        window_seconds=10,
        datetime_provider=datetime_provider,
    )
    with limiter.context():
        pass
    with limiter.context():
        pass


def test_sliding_window_failed_attempt_counts() -> None:
    datetime_provider = unittest.mock.Mock(spec=dl_httpx.DateTimeProvider)
    datetime_provider.get_now.return_value = 0.0
    limiter = dl_httpx.SlidingWindowRateLimiter(
        max_requests=1,
        window_seconds=60,
        datetime_provider=datetime_provider,
    )
    with pytest.raises(RuntimeError):
        with limiter.context():
            raise RuntimeError("fail")
    with pytest.raises(dl_httpx.RateLimitHttpxClientException):
        with limiter.context():
            pass
