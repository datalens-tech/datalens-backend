import uuid

import pytest
import pytest_asyncio

import dl_us_entries_client


@pytest.fixture
def entry_data() -> dl_us_entries_client.EntryData:
    return dl_us_entries_client.EntryData(
        scope=dl_us_entries_client.EntryScope.dashboard,
        key=str(uuid.uuid4()),
    )


@pytest_asyncio.fixture
async def entry_in_us(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
    entry_data: dl_us_entries_client.EntryData,
) -> dl_us_entries_client.Entry:
    return await us_entries_client.post_entry(dl_us_entries_client.EntryPostRequest(entry=entry_data))


@pytest.mark.asyncio
async def test_get_entry(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    response = await us_entries_client.get_entry(dl_us_entries_client.EntryGetRequest(entry_id=entry_in_us.entry_id))
    assert response == entry_in_us


@pytest.mark.asyncio
async def test_post_entry(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
    entry_data: dl_us_entries_client.EntryData,
) -> None:
    response = await us_entries_client.post_entry(dl_us_entries_client.EntryPostRequest(entry=entry_data))
    assert response.entry_id is not None


@pytest.mark.asyncio
async def test_delete_entry(
    us_entries_client: dl_us_entries_client.USEntriesAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    await us_entries_client.delete_entry(dl_us_entries_client.EntryDeleteRequest(entry_id=entry_in_us.entry_id))

    with pytest.raises(dl_us_entries_client.EntryNotFoundError):
        await us_entries_client.get_entry(dl_us_entries_client.EntryGetRequest(entry_id=entry_in_us.entry_id))
