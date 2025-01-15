from copy import deepcopy

import attr
import pytest

from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)


@attr.s
class Level1EntrySchemaMigration(BaseEntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                "2022-12-04 13:00:00",
                "Second level 1 migration",
                Level1EntrySchemaMigration._migrate_v2_to_v3,
                Level1EntrySchemaMigration._migrate_v2_to_v3_async,
            ),
            Migration(
                "2022-12-01 12:00:00",
                "First level 1 migration",
                Level1EntrySchemaMigration._migrate_v1_to_v2,
                Level1EntrySchemaMigration._migrate_v1_to_v2_async,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v1_to_v2(entry: dict) -> dict:
        entry["data"]["new_field"] = entry["data"].pop("old_field", "default_value")
        return entry

    @staticmethod
    async def _migrate_v1_to_v2_async(entry: dict) -> dict:
        return Level1EntrySchemaMigration._migrate_v1_to_v2(entry)

    @staticmethod
    def _migrate_v2_to_v3(entry: dict) -> dict:
        entry["data"]["l1_field"] = "added_in_l1"
        return entry

    @staticmethod
    async def _migrate_v2_to_v3_async(entry: dict) -> dict:
        return Level1EntrySchemaMigration._migrate_v2_to_v3(entry)


@attr.s
class Level2EntrySchemaMigration(Level1EntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                "2022-12-03 13:00:00",
                "Second level 2 migration",
                Level2EntrySchemaMigration._migrate_v2_to_v3,
            ),
            Migration(
                "2022-12-02 12:00:00",
                "First level 2 migration",
                Level2EntrySchemaMigration._migrate_v1_to_v2,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v1_to_v2(entry: dict) -> dict:
        entry["data"]["new_field"] = "default_value"
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict) -> dict:
        entry["data"]["l2_field"] = "added_in_l2"
        return entry


@attr.s
class Level3EntrySchemaMigration(Level2EntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                "2022-12-03 13:00:00",
                "Third level 2 migration",
                Level3EntrySchemaMigration._migrate_v3_to_v1,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v3_to_v1(entry: dict) -> dict:
        entry["data"]["abs_field"] = "one more new value"
        return entry


@pytest.fixture
def l2_migrator():
    return Level2EntrySchemaMigration(strict_migration=True)


@pytest.fixture
def l3_migrator():
    return Level3EntrySchemaMigration(strict_migration=True)


@pytest.fixture
def l3_nonstrict_migrator():
    return Level3EntrySchemaMigration()


def test_successful_migration(l2_migrator):
    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    expected = {
        "data": {
            "new_field": "default_value",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-04 13:00:00",
        }
    }
    result = l2_migrator.migrate(entry)
    assert result == expected


def test_no_migration_needed(l2_migrator):
    entry = {
        "data": {
            "new_field": "default_value",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-04 13:00:00",
        }
    }
    result = l2_migrator.migrate(entry)
    assert result == entry


def test_invalid_data_format(l2_migrator):
    entry = {
        "data": "invalid_data_format",
    }
    with pytest.raises(ValueError, match="Invalid entry: 'data' should be a dict"):
        l2_migrator.migrate(entry)


def test_missing_data_key(l2_migrator):
    entry = {}
    with pytest.raises(ValueError, match="Invalid entry: 'data' should be a dict"):
        l2_migrator.migrate(entry)


def test_cyclic_migration(l3_migrator):
    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    with pytest.raises(ValueError, match="Double migration detected"):
        l3_migrator.migrate(entry)


def test_migration_failure(l3_nonstrict_migrator):
    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    original_entry = deepcopy(entry)

    result = l3_nonstrict_migrator.migrate(entry)
    assert result == original_entry


@pytest.mark.parametrize(
    "migration_version",
    (
        "some string",
        "2022-13-03 23:00:00",
        "2022-12-40 23:00:00",
        "2022-12-03 33:00:00",
    ),
)
def test_wrong_migration_version(migration_version):
    with pytest.raises(ValueError):
        Migration(
            migration_version,
            "Broken version migration",
            lambda x: x,
        )
