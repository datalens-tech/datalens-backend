from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import attr
import pytest

from dl_constants.enums import MigrationStatus
from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_tests.db.base import DefaultCoreTestClass


if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


@attr.s
class SimpleEntrySchemaMigration(BaseEntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                datetime(2025, 1, 1, 12, 0, 0),
                "Test migration",
                up_function=SimpleEntrySchemaMigration._migrate_v1_to_v2,
                down_function=SimpleEntrySchemaMigration._migrate_v2_to_v1,
                downgrade_only=False,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v1_to_v2(entry: dict, **kwargs) -> dict:
        entry["meta"]["test_field"] = "test_value"
        return entry

    @staticmethod
    def _migrate_v2_to_v1(entry: dict, **kwargs) -> dict:
        entry["meta"].pop("test_field")
        return entry


class SimpleEntrySchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
        service_registry: ServicesRegistry | None = None,
    ) -> BaseEntrySchemaMigration:
        return SimpleEntrySchemaMigration()


class TestMigration(DefaultCoreTestClass):
    @pytest.fixture(scope="function")
    def sync_us_migration_manager(self, conn_default_sync_us_manager: SyncUSManager) -> SyncUSManager:
        return conn_default_sync_us_manager.clone(
            schema_migration_factory=SimpleEntrySchemaMigrationFactory(),
        )

    def test_migration(self, sync_us_migration_manager, saved_dataset):
        dataset = saved_dataset
        assert "test_field" not in dataset.meta
        sync_us_migration_manager.save(dataset)
        dataset = sync_us_migration_manager.get_by_id(entry_id=dataset.uuid)
        assert "test_field" in dataset.meta
        assert dataset.meta["test_field"] == "test_value"
        assert dataset.migration_status == MigrationStatus.migrated_up
