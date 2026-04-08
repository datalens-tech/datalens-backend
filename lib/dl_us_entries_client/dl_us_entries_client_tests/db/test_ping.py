import pytest

import dl_us_entries_client


@pytest.mark.asyncio
async def test_ping(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
) -> None:
    response = await us_entries_client.ping(dl_us_entries_client.PingRequest())
    assert response.result == "pong"


@pytest.mark.asyncio
async def test_check_readiness(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
) -> None:
    result = await us_entries_client.check_readiness()
    assert result is True
