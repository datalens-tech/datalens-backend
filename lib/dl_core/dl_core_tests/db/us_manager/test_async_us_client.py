from __future__ import annotations

from importlib.resources import files

import pytest
import pytest_asyncio
import shortuuid

from dl_core.base_models import PathEntryLocation
from dl_core.exc import USLockUnacquiredError
from dl_core.united_storage_client_aio import UStorageClientAIO
from dl_core.us_manager.dynamic_token_factory import DynamicUSMasterTokenFactory
import dl_retrier


@pytest_asyncio.fixture(scope="function")
async def us_client(loop, core_test_config, root_certificates) -> UStorageClientAIO:
    us_config = core_test_config.get_us_config()
    private_key = (files("dl_core_testing") / "keys" / "dynamic_us_master_token_private_key.pem").read_text()
    dynamic_token_factory = DynamicUSMasterTokenFactory(
        private_key=private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    client = UStorageClientAIO(
        host=us_config.us_host,
        auth_ctx=dynamic_token_factory.get_auth_context(us_master_token=us_config.us_master_token),
        prefix=None,
        ca_data=root_certificates,
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
    )
    yield client
    await client.close()


@pytest.mark.asyncio
@pytest_asyncio.fixture(scope="function")
async def test_entry_id(us_client) -> str:
    create_resp = await us_client.create_entry(
        scope="connection",
        key=PathEntryLocation(f"remove_me/{shortuuid.uuid()}"),
    )
    entry_id = create_resp["entryId"]
    yield entry_id
    lock = await us_client.acquire_lock(entry_id, force=True)
    await us_client.delete_entry(entry_id, lock)


@pytest.mark.asyncio
async def test_lock_not_acquired(us_client, test_entry_id):
    initial_lock = await us_client.acquire_lock(test_entry_id, duration=1)
    try:
        # Check for lock
        with pytest.raises(USLockUnacquiredError):
            await us_client.acquire_lock(test_entry_id, duration=1)
    finally:
        await us_client.release_lock(test_entry_id, initial_lock)


@pytest.mark.asyncio
async def test_wait_lock(us_client, test_entry_id):
    # Check for lock and wait
    timed_out_lock = await us_client.acquire_lock(test_entry_id, duration=1)
    try:
        next_lock = await us_client.acquire_lock(test_entry_id, wait_timeout=2)
        assert next_lock
    finally:
        await us_client.release_lock(test_entry_id, timed_out_lock)
