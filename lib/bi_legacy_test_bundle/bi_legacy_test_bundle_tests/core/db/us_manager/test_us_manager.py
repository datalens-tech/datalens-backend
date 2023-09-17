from __future__ import annotations

from typing import Optional

import pytest
from aiohttp import ClientResponseError

from bi_constants.enums import DataSourceRole

from bi_core import exc
from bi_core.data_source import DataSource
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager import USManagerBase
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.dataset_wrappers import DatasetTestWrapper
from bi_core_testing.connection import make_connection

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse


def make_test_connection(usm: USManagerBase) -> ConnectionClickhouse:
    conn = make_connection(us_manager=usm, conn_type=CONNECTION_TYPE_CLICKHOUSE)
    assert isinstance(conn, ConnectionClickhouse)
    return conn


@pytest.mark.asyncio
async def test_us_manager_lock(local_private_usm: AsyncUSManager):
    usm = local_private_usm
    original_conn = make_test_connection(usm)
    await usm.save(original_conn)

    try:
        async with usm.locked_entry_cm(
            original_conn.uuid, ConnectionClickhouse, duration_sec=10
        ) as locked_conn:
            reloaded_conn = await usm.get_by_id(original_conn.uuid, ConnectionClickhouse)
            reloaded_conn.data.name = "ololo"

            with pytest.raises(exc.USLockUnacquiredException):
                try:
                    await usm.save(reloaded_conn)
                except exc.USLockUnacquiredException as e:
                    assert isinstance(e.orig_exc, ClientResponseError)
                    assert e.orig_exc.status == 423
                    raise

            locked_conn.data.name = "azaza"
            await usm.save(locked_conn)

    finally:
        await usm.delete(original_conn)


# noinspection PyProtectedMember
def test_us_manager_lock_sync(sync_usm: SyncUSManager):
    orig_entry = make_test_connection(usm=sync_usm)
    sync_usm.save(orig_entry)

    # Manual locks
    #
    lock_token = sync_usm.acquire_lock(orig_entry)
    # Ensure lock token was saved
    assert isinstance(lock_token, str) and lock_token and lock_token == orig_entry._lock
    # Reloading entry
    reloaded_entry = sync_usm.get_by_id(orig_entry.uuid)
    # Ensure lock works
    with pytest.raises(exc.USLockUnacquiredException):
        reloaded_entry.data.host = "ololo"
        sync_usm.save(reloaded_entry)
    # Ensure we can save originally locked entry
    orig_entry.data.host = "azaza"
    sync_usm.save(orig_entry)
    sync_usm.release_lock(orig_entry)
    # Ensure lock token was removed
    assert orig_entry._lock is None
    # Ensure locked entry was released
    sync_usm.save(reloaded_entry)

    # Get locked CM
    #
    with sync_usm.get_locked_entry_cm(ConnectionClickhouse, orig_entry.uuid) as locked_entry:
        assert isinstance(locked_entry._lock, str)
        # Reloading entry
        reloaded_entry = sync_usm.get_by_id(orig_entry.uuid)
        # Ensure lock works
        with pytest.raises(exc.USLockUnacquiredException):
            reloaded_entry.data.host = "ololo_get_locked_cm"
            sync_usm.save(reloaded_entry)

    # Ensure locked entry was released
    sync_usm.save(reloaded_entry)

    # Locked CM
    #
    locked_entry = sync_usm.get_by_id(orig_entry.uuid, expected_type=ConnectionClickhouse)

    with sync_usm.locked_cm(locked_entry):
        assert isinstance(locked_entry._lock, str)
        # Reloading entry
        reloaded_entry = sync_usm.get_by_id(orig_entry.uuid)
        # Ensure lock works
        with pytest.raises(exc.USLockUnacquiredException):
            reloaded_entry.data.host = "ololo_just_locked_cm"
            sync_usm.save(reloaded_entry)

    # Ensure locked entry was released
    sync_usm.save(reloaded_entry)


# noinspection PyProtectedMember
@pytest.mark.asyncio
async def test_us_manager_links_load(saved_dataset, local_private_usm: AsyncUSManager):
    usm = local_private_usm
    dataset = await usm.get_by_id(saved_dataset.uuid, Dataset)
    await usm.load_dependencies(dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=usm)

    loaded_connection_ref_set = set(usm._loaded_entries.keys())
    manually_collected_related_conn_set = set()

    def add_connection_from_data_source(dsrc: Optional[DataSource]):
        if dsrc is not None and dsrc.connection is not None:
            manually_collected_related_conn_set.add(dsrc.connection.conn_ref)

    for dsrc_coll in ds_wrapper.get_data_source_coll_list():
        add_connection_from_data_source(dsrc_coll.get_strict(DataSourceRole.origin))
        add_connection_from_data_source(dsrc_coll.get_opt(DataSourceRole.materialization))
        add_connection_from_data_source(dsrc_coll.get_opt(DataSourceRole.sample))

    assert (
        len(manually_collected_related_conn_set) > 0
        and loaded_connection_ref_set == manually_collected_related_conn_set
    )


@pytest.mark.asyncio
async def test_correct_exception_on_missing_dependency(saved_dataset, local_private_usm: AsyncUSManager):
    # Looking for referenced connection ID
    us_manager = local_private_usm
    dataset = saved_dataset
    lifecycle_manager = us_manager.get_lifecycle_manager(dataset)
    referenced_conn_id_set = set(lifecycle_manager.collect_links().values())
    assert len(referenced_conn_id_set) == 1, \
        "Unexpected count of referenced connections in dataset fixture saved_dataset"
    referenced_conn_id = next(iter(referenced_conn_id_set))

    # Deleting referenced connection
    await us_manager._us_client.delete_entry(referenced_conn_id)

    loaded_dataset = await us_manager.get_by_id(dataset.uuid, Dataset)
    await us_manager.load_dependencies(loaded_dataset)

    with pytest.raises(exc.ReferencedUSEntryNotFound, match=f'.+ {referenced_conn_id} .+'):
        us_manager.get_loaded_us_connection(referenced_conn_id)
