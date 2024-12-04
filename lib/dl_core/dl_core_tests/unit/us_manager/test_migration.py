from copy import deepcopy
from typing import Callable

import attr
import pytest

from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration


@attr.s
class TestEntrySchemaMigration(BaseEntrySchemaMigration):
    _MIGRATIONS: dict[str, Callable[[dict], dict]] = {
        "1": lambda entry: TestEntrySchemaMigration._migrate_v1_to_v2(entry),
        "2": lambda entry: TestEntrySchemaMigration._migrate_v2_to_v3(entry),
    }

    @staticmethod
    def _migrate_v1_to_v2(entry: dict) -> dict:
        entry["data"]["new_field"] = entry["data"].pop("old_field", "default_value")
        entry["data"]["schema_version"] = "2"
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict) -> dict:
        entry["data"]["another_field"] = "added_in_v3"
        entry["data"]["schema_version"] = "3"
        return entry


@pytest.fixture
def migrator():
    return TestEntrySchemaMigration(strict_migration=True)


@pytest.fixture
def nonstrict_migrator():
    return TestEntrySchemaMigration()


def test_successful_migration(migrator):
    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    expected = {
        "data": {
            "new_field": "value1",
            "another_field": "added_in_v3",
            "schema_version": "3",
        }
    }
    result = migrator.migrate(entry)
    assert result == expected


def test_no_migration_needed(migrator):
    entry = {
        "data": {
            "new_field": "value1",
            "another_field": "added_in_v3",
            "schema_version": "3",
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
        entry["data"]["schema_version"] = "1"
        return entry

    migrator.__class__._MIGRATIONS["3"] = _migrate_v3_to_v1

    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }

    with pytest.raises(ValueError, match="Cyclic migration detected for schema_version: 1"):
        migrator.migrate(entry)


def test_migration_failure(nonstrict_migrator):
    @staticmethod
    def broken_migration(entry: dict) -> dict:
        raise RuntimeError("Migration failed")

    nonstrict_migrator.__class__._MIGRATIONS["2"] = broken_migration

    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    original_entry = deepcopy(entry)

    result = nonstrict_migrator.migrate(entry)
    assert result == original_entry
