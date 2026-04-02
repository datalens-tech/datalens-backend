import pytest

import dl_httpx


def test_no_rate_limiter_sync() -> None:
    settings = dl_httpx.RateLimiterSettings.factory({"TYPE": "NONE"})
    assert isinstance(settings, dl_httpx.NoRateLimiterSettings)
    limiter = dl_httpx.NoRateLimiter.from_settings(settings)
    with limiter.context():
        pass


@pytest.mark.asyncio
async def test_no_rate_limiter_async() -> None:
    settings = dl_httpx.RateLimiterSettings.factory({"TYPE": "NONE"})
    assert isinstance(settings, dl_httpx.NoRateLimiterSettings)
    limiter = dl_httpx.NoRateLimiter.from_settings(settings)
    async with limiter.context_async():
        pass
