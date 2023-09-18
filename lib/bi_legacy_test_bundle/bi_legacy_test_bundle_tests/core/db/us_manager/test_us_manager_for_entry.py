from __future__ import annotations

from typing import Callable

import pytest

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_connection


class BaseTestUSMForEntry:
    sensitive_fields = ()

    @pytest.fixture()
    def sync_usm(self, sync_usm) -> SyncUSManager:
        return sync_usm

    @pytest.fixture()
    def async_usm(self, local_private_usm) -> AsyncUSManager:
        return local_private_usm

    @pytest.fixture()
    def entry_factory(self) -> Callable[[USManagerBase], USEntry]:
        raise NotImplementedError()

    @pytest.fixture()
    def entry_sync_usm(self, entry_factory, sync_usm):
        return entry_factory(sync_usm)

    @pytest.fixture()
    def entry_async_usm(self, entry_factory, async_usm):
        return entry_factory(async_usm)

    @pytest.mark.asyncio
    async def test_read_write_async(self, entry_async_usm, async_usm):
        original_entry = entry_async_usm
        us_mgr = async_usm

        await us_mgr.save(original_entry)

        # noinspection PyProtectedMember
        assert original_entry._stored_in_db is True
        assert original_entry.uuid is not None
        assert original_entry._us_resp is not None

        loaded_entry = await us_mgr.get_by_id(original_entry.uuid, type(original_entry))

        assert original_entry is not loaded_entry
        assert type(original_entry) == type(loaded_entry)
        assert original_entry.data == loaded_entry.data
        assert loaded_entry._us_manager is us_mgr

    def test_read_write_sync(self, entry_sync_usm, sync_usm):
        original_entry = entry_sync_usm
        us_mgr = sync_usm

        assert original_entry._stored_in_db is False

        us_mgr.save(original_entry)

        assert original_entry._stored_in_db is True
        assert original_entry.uuid is not None
        assert original_entry._us_resp is not None

        loaded_entry = us_mgr.get_by_id(original_entry.uuid, type(original_entry))

        assert original_entry is not loaded_entry
        assert type(original_entry) == type(loaded_entry)
        assert original_entry.data == loaded_entry.data
        assert loaded_entry._us_manager is us_mgr

    def test_migration_entry(self, entry_sync_usm, sync_usm):
        initial_entry = entry_sync_usm
        sync_usm.save(initial_entry)

        raw_entry = sync_usm.get_by_id(initial_entry.uuid, expected_type=USMigrationEntry)
        raw_entry.unversioned_data["new_unversioned_field"] = "new_unversioned_val"
        raw_entry.data["new_data_field"] = "new_data_val"
        sync_usm.save(raw_entry)

        reloaded_raw_entry = sync_usm.get_by_id(initial_entry.uuid, expected_type=USMigrationEntry)
        assert raw_entry is not reloaded_raw_entry

        assert reloaded_raw_entry.data["new_data_field"] == "new_data_val"
        assert reloaded_raw_entry.data == raw_entry.data

        assert reloaded_raw_entry.unversioned_data["new_unversioned_field"] == "new_unversioned_val"
        assert reloaded_raw_entry.unversioned_data == raw_entry.unversioned_data

    def test_encryption_sync_usm(self, entry_sync_usm, sync_usm):
        if not self.sensitive_fields:
            raise pytest.skip("Entry has not sensitive fields")

        initial_entry = entry_sync_usm

        assert initial_entry._stored_in_db is False
        sync_usm.save(initial_entry)

        raw_entry = sync_usm.get_by_id(initial_entry.uuid, expected_type=USMigrationEntry)

        crypto_controller = sync_usm._crypto_controller

        for field in self.sensitive_fields:
            plain_text_value = getattr(initial_entry.data, field)

            encrypted_value_in_unversioned = raw_entry.unversioned_data[field]
            assert crypto_controller.decrypt(encrypted_value_in_unversioned) == plain_text_value

            # Check that there is no sensitive field in versioned data
            assert field not in raw_entry.data

    @pytest.mark.asyncio
    async def test_encryption_async_usm(self, entry_async_usm, async_usm):
        if not self.sensitive_fields:
            raise pytest.skip("Entry has not sensitive fields")

        initial_entry = entry_async_usm

        assert initial_entry._stored_in_db is False
        await async_usm.save(initial_entry)

        raw_entry = await async_usm.get_by_id(initial_entry.uuid, expected_type=USMigrationEntry)

        crypto_controller = async_usm._crypto_controller

        for field in self.sensitive_fields:
            plain_text_value = getattr(initial_entry.data, field)

            encrypted_value_in_unversioned = raw_entry.unversioned_data[field]
            assert crypto_controller.decrypt(encrypted_value_in_unversioned) == plain_text_value

            # Check that there is no sensitive field in versioned data
            assert field not in raw_entry.data


class TestUSMForConnectionPostgres(BaseTestUSMForEntry):
    sensitive_fields = ("password",)

    @pytest.fixture()
    def entry_factory(self, postgres_db) -> Callable[[USManagerBase], ConnectionPostgreSQL]:
        def factory(usm):
            return make_connection(usm, postgres_db)

        return factory


class TestUSMForConnectionClickhouse(BaseTestUSMForEntry):
    sensitive_fields = ("password",)

    @pytest.fixture()
    def entry_factory(self, clickhouse_db) -> Callable[[USManagerBase], ConnectionClickhouse]:
        def factory(usm):
            return make_connection(usm, clickhouse_db)

        return factory
