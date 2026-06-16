import uuid

import pytest
import pytest_asyncio

import dl_json
import dl_us_entries_client


@pytest.fixture
def entry_data() -> dl_us_entries_client.EntryData:
    return dl_us_entries_client.EntryData(
        scope=dl_us_entries_client.EntryScope.dataset,
        key=str(uuid.uuid4()),
    )


@pytest_asyncio.fixture
async def entry_in_us(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_data: dl_us_entries_client.EntryData,
) -> dl_us_entries_client.Entry:
    return await us_entries_private_client.post_entry(
        dl_us_entries_client.PrivateEntryPostRequest(entry=entry_data),
    )


@pytest.mark.asyncio
async def test_get_entry(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    response = await us_entries_private_client.get_entry(
        dl_us_entries_client.PrivateEntryGetRequest(entry_id=entry_in_us.entry_id),
    )
    assert response.model_dump() == entry_in_us.model_dump()


@pytest.mark.asyncio
async def test_post_entry(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_data: dl_us_entries_client.EntryData,
) -> None:
    response = await us_entries_private_client.post_entry(
        dl_us_entries_client.PrivateEntryPostRequest(entry=entry_data),
    )
    assert response.entry_id is not None


@pytest.mark.asyncio
async def test_delete_entry(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    await us_entries_private_client.delete_entry(
        dl_us_entries_client.PrivateEntryDeleteRequest(entry_id=entry_in_us.entry_id),
    )

    with pytest.raises(dl_us_entries_client.NotFoundError):
        await us_entries_private_client.get_entry(
            dl_us_entries_client.PrivateEntryGetRequest(entry_id=entry_in_us.entry_id),
        )


@pytest.mark.asyncio
async def test_post_unversioned_data(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    new_unversioned_data: dl_json.JsonSerializableMapping = {
        "foo": "bar",
        "answer": 42,
    }

    response = await us_entries_private_client.post_unversioned_data(
        dl_us_entries_client.PrivateEntryUnversionedDataPostRequest(
            entry_id=entry_in_us.entry_id,
            unversioned_data=new_unversioned_data,
        ),
    )
    assert response.entry_id == entry_in_us.entry_id
    assert response.unversioned_data == new_unversioned_data

    fetched = await us_entries_private_client.get_entry(
        dl_us_entries_client.PrivateEntryGetRequest(entry_id=entry_in_us.entry_id),
    )
    assert fetched.unversioned_data is not None
    assert fetched.unversioned_data.model_dump() == new_unversioned_data
