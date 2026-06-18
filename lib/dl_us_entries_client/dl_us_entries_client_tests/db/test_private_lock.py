import datetime
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
async def test_delete_locked_entry(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    # Make a lock
    lock = await us_entries_private_client.post_lock(
        dl_us_entries_client.PrivateEntryLockPostRequest(
            entry_id=entry_in_us.entry_id,
            duration=datetime.timedelta(minutes=10),
        ),
    )

    # Delete without token = not OK
    with pytest.raises(dl_us_entries_client.EntryLockedError):
        await us_entries_private_client.delete_entry(
            dl_us_entries_client.PrivateEntryDeleteRequest(
                entry_id=entry_in_us.entry_id,
            ),
        )

    # Delete with invalid token = not OK
    with pytest.raises(dl_us_entries_client.EntryLockedError):
        await us_entries_private_client.delete_entry(
            dl_us_entries_client.PrivateEntryDeleteRequest(
                entry_id=entry_in_us.entry_id,
                lock_token="invalid-lock-token",
            ),
        )

    # Delete with valid token = OK
    await us_entries_private_client.delete_entry(
        dl_us_entries_client.PrivateEntryDeleteRequest(
            entry_id=entry_in_us.entry_id,
            lock_token=lock.lock_token,
        ),
    )

    # Check after delete
    with pytest.raises(dl_us_entries_client.NotFoundError):
        await us_entries_private_client.get_entry(
            dl_us_entries_client.PrivateEntryGetRequest(entry_id=entry_in_us.entry_id),
        )


@pytest.mark.asyncio
async def test_lock_lifecycle(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    # Single lock = OK
    lock = await us_entries_private_client.post_lock(
        dl_us_entries_client.PrivateEntryLockPostRequest(
            entry_id=entry_in_us.entry_id,
            duration=datetime.timedelta(minutes=10),
        ),
    )

    # Double lock = error
    with pytest.raises(dl_us_entries_client.EntryLockedError):
        await us_entries_private_client.post_lock(
            dl_us_entries_client.PrivateEntryLockPostRequest(
                entry_id=entry_in_us.entry_id,
                duration=datetime.timedelta(minutes=10),
            ),
        )

    # Delete lock = OK
    released = await us_entries_private_client.delete_lock(
        dl_us_entries_client.PrivateEntryLockDeleteRequest(
            entry_id=entry_in_us.entry_id,
            lock_token=lock.lock_token,
        ),
    )
    assert released.entry_id == entry_in_us.entry_id
    assert released.lock_token == lock.lock_token

    # Double delete lock = not OK
    with pytest.raises(dl_us_entries_client.NotFoundError):
        await us_entries_private_client.delete_lock(
            dl_us_entries_client.PrivateEntryLockDeleteRequest(
                entry_id=entry_in_us.entry_id,
                lock_token=lock.lock_token,
            ),
        )


@pytest.mark.asyncio
async def test_post_unversioned_data_under_lock(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    new_unversioned_data: dl_json.JsonSerializableMapping = {
        "foo": "bar",
        "answer": 42,
    }

    # Make a lock
    lock = await us_entries_private_client.post_lock(
        dl_us_entries_client.PrivateEntryLockPostRequest(
            entry_id=entry_in_us.entry_id,
            duration=datetime.timedelta(minutes=10),
        ),
    )

    # Write without token = not OK
    with pytest.raises(dl_us_entries_client.EntryLockedError):
        await us_entries_private_client.post_unversioned_data(
            dl_us_entries_client.PrivateEntryUnversionedDataPostRequest(
                entry_id=entry_in_us.entry_id,
                unversioned_data=new_unversioned_data,
            ),
        )

    # Write with invalid token = not OK
    with pytest.raises(dl_us_entries_client.EntryLockedError):
        await us_entries_private_client.post_unversioned_data(
            dl_us_entries_client.PrivateEntryUnversionedDataPostRequest(
                entry_id=entry_in_us.entry_id,
                unversioned_data=new_unversioned_data,
                lock_token="invalid-lock-token",
            ),
        )

    # Write with valid token = OK
    response = await us_entries_private_client.post_unversioned_data(
        dl_us_entries_client.PrivateEntryUnversionedDataPostRequest(
            entry_id=entry_in_us.entry_id,
            unversioned_data=new_unversioned_data,
            lock_token=lock.lock_token,
        ),
    )
    assert response.entry_id == entry_in_us.entry_id
    assert response.unversioned_data == new_unversioned_data


@pytest.mark.asyncio
async def test_lock_nonexisting(
    us_entries_private_client: dl_us_entries_client.USEntriesPrivateAsyncClient,
    entry_in_us: dl_us_entries_client.Entry,
) -> None:
    # Lock nonexisting
    with pytest.raises(dl_us_entries_client.BadRequestError):
        await us_entries_private_client.post_lock(
            dl_us_entries_client.PrivateEntryLockPostRequest(
                entry_id="nonexisting",
                duration=datetime.timedelta(minutes=1),
            ),
        )
