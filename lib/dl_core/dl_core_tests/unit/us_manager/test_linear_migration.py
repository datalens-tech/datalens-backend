from copy import deepcopy

import attr
import pytest

from dl_core.us_manager.schema_migration.base import (
    BaseLinearEntrySchemaMigration,
    Migration,
)


@attr.s
class Level1EntrySchemaMigration(BaseLinearEntrySchemaMigration):
    _MIGRATIONS = [
        Migration(
            "2022-12-04 13:00:00",
            "Second level 1 migration",
            lambda entry: Level1EntrySchemaMigration._migrate_v2_to_v3(entry),
        ),
        Migration(
            "2022-12-01 12:00:00",
            "First level 1 migration",
            lambda entry: Level1EntrySchemaMigration._migrate_v1_to_v2(entry),
        ),
    ]

    @staticmethod
    def _migrate_v1_to_v2(entry: dict) -> dict:
        entry["data"]["new_field"] = entry["data"].pop("old_field", "default_value")
        entry["data"]["schema_version"] = "2022-12-01 12:00:00"
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict) -> dict:
        entry["data"]["l1_field"] = "added_in_l1"
        entry["data"]["schema_version"] = "2022-12-04 13:00:00"
        return entry


@attr.s
class Level2EntrySchemaMigration(Level1EntrySchemaMigration):
    _MIGRATIONS = [
        Migration(
            "2022-12-03 13:00:00",
            "Second level 2 migration",
            lambda entry: Level2EntrySchemaMigration._migrate_v2_to_v3(entry),
        ),
        Migration(
            "2022-12-02 12:00:00",
            "First level 2 migration",
            lambda entry: Level2EntrySchemaMigration._migrate_v1_to_v2(entry),
        ),
    ]

    @staticmethod
    def _migrate_v1_to_v2(entry: dict) -> dict:
        entry["data"]["new_field"] = "default_value"
        entry["data"]["schema_version"] = "2022-12-02 12:00:00"
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict) -> dict:
        entry["data"]["l2_field"] = "added_in_l2"
        entry["data"]["schema_version"] = "2022-12-03 13:00:00"
        return entry


@pytest.fixture
def migrator():
    return Level2EntrySchemaMigration(strict_migration=True)


@pytest.fixture
def nonstrict_migrator():
    return Level2EntrySchemaMigration()


def test_successful_migration(migrator):
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
    result = migrator.migrate(entry)
    assert result == expected


def test_no_migration_needed(migrator):
    entry = {
        "data": {
            "new_field": "default_value",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-04 13:00:00",
        }
    }
    result = migrator.migrate(entry)
    assert result == entry


def test_invalid_data_format(migrator):
    entry = {
        "data": "invalid_data_format",
    }
    with pytest.raises(ValueError, match="Invalid entry: 'data' should be a dict"):
        migrator.migrate(entry)


def test_missing_data_key(migrator):
    entry = {}
    with pytest.raises(ValueError, match="Invalid entry: 'data' should be a dict"):
        migrator.migrate(entry)


def test_cyclic_migration(migrator):
    @staticmethod
    def _migrate_v3_to_v1(entry: dict) -> dict:
        entry["data"]["schema_version"] = "2022-12-03 13:00:00"
        return entry

    migrator.__class__._MIGRATIONS.append(
        Migration("2022-12-03 13:00:00", "Third level 2 migration", _migrate_v3_to_v1)
    )

    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }

    with pytest.raises(ValueError, match="Double migration detected"):
        migrator.migrate(entry)


def test_migration_failure(nonstrict_migrator):
    @staticmethod
    def broken_migration(entry: dict) -> dict:
        raise RuntimeError("Migration failed")

    nonstrict_migrator.__class__._MIGRATIONS.append(
        Migration("2022-12-03 13:00:00", "Third level 2 migration", broken_migration)
    )

    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    original_entry = deepcopy(entry)

    result = nonstrict_migrator.migrate(entry)
    assert result == original_entry
