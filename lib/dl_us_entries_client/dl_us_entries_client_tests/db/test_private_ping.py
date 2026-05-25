import pytest

import dl_us_entries_client


@pytest.mark.asyncio
async def test_ping(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
) -> None:
    await us_entries_private_client.ping(dl_us_entries_client.PingRequest())


@pytest.mark.asyncio
async def test_check_readiness(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
) -> None:
    result = await us_entries_private_client.check_readiness()
    assert result is True
