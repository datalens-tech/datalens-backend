from datetime import datetime

import attr
import pytest

from dl_core.exc import UnknownEntryMigration
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
                datetime(2022, 12, 4, 13, 0, 0),
                "Second level 1 migration",
                up_function=Level1EntrySchemaMigration._migrate_v2_to_v3,
                down_function=Level1EntrySchemaMigration._migrate_v3_to_v2,
                downgrade_only=False,
            ),
            Migration(
                datetime(2022, 12, 1, 12, 0, 0),
                "First level 1 migration",
                up_function=Level1EntrySchemaMigration._migrate_v1_to_v2,
                down_function=Level1EntrySchemaMigration._migrate_v2_to_v1,
                downgrade_only=False,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v1_to_v2(entry: dict, **kwargs) -> dict:
        entry["data"]["new_field"] = entry["data"].pop("old_field", "default_value")
        return entry

    @staticmethod
    def _migrate_v2_to_v1(entry: dict, **kwargs) -> dict:
        entry["data"]["old_field"] = entry["data"].pop("new_field", "default_value")
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict, **kwargs) -> dict:
        entry["data"]["l1_field"] = "added_in_l1"
        return entry

    @staticmethod
    def _migrate_v3_to_v2(entry: dict, **kwargs) -> dict:
        entry["data"].pop("l1_field")
        return entry


@attr.s
class Level2EntrySchemaMigration(Level1EntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                datetime(2022, 12, 3, 13, 0, 0),
                "Second level 2 migration",
                up_function=Level2EntrySchemaMigration._migrate_v2_to_v3,
                down_function=Level2EntrySchemaMigration._migrate_v3_to_v2,
                downgrade_only=False,
            ),
            Migration(
                datetime(2022, 12, 2, 12, 0, 0),
                "First level 2 migration",
                up_function=Level2EntrySchemaMigration._migrate_v1_to_v2,
                down_function=Level2EntrySchemaMigration._migrate_v2_to_v1,
                downgrade_only=False,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v1_to_v2(entry: dict, **kwargs) -> dict:
        entry["data"]["new_field"] = "default_value"
        return entry

    @staticmethod
    def _migrate_v2_to_v1(entry: dict, **kwargs) -> dict:
        entry["data"].pop("new_field")
        return entry

    @staticmethod
    def _migrate_v2_to_v3(entry: dict, **kwargs) -> dict:
        entry["data"]["l2_field"] = "added_in_l2"
        return entry

    @staticmethod
    def _migrate_v3_to_v2(entry: dict, **kwargs) -> dict:
        entry["data"].pop("l2_field")
        return entry


@attr.s
class Level2EntrySchemaMigrationDowngradeOnly(Level1EntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                datetime(2022, 12, 5, 13, 0, 0),
                "Second level 2 migration",
                up_function=Level2EntrySchemaMigration._migrate_v2_to_v3,
                down_function=Level2EntrySchemaMigration._migrate_v3_to_v2,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v2_to_v3(entry: dict, **kwargs) -> dict:
        entry["data"]["l2_field"] = "added_in_l2"
        return entry

    @staticmethod
    def _migrate_v3_to_v2(entry: dict, **kwargs) -> dict:
        entry["data"].pop("l2_field")
        return entry


@attr.s
class Level3EntrySchemaMigration(Level2EntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                datetime(2022, 12, 3, 13, 0, 0),
                "Third level 2 migration",
                up_function=Level3EntrySchemaMigration._migrate_v3_to_v4,
                down_function=Level3EntrySchemaMigration._migrate_v4_to_v3,
                downgrade_only=False,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _migrate_v3_to_v4(entry: dict) -> dict:
        entry["data"]["abs_field"] = "one more new value"
        return entry

    @staticmethod
    def _migrate_v4_to_v3(entry: dict) -> dict:
        entry["data"].pop("abs_field")
        return entry


@pytest.fixture
def l2_migrator():
    return Level2EntrySchemaMigration(strict_migration=True)


@pytest.fixture
def l2_downgrade_migrator():
    return Level2EntrySchemaMigrationDowngradeOnly(strict_migration=True)


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
            "schema_version": "2022-12-04T13:00:00",
        },
        "migration_status": "migrated_up",
    }
    result = l2_migrator.migrate(entry)
    assert result == expected


def test_no_migration_needed(l2_migrator):
    entry = {
        "data": {
            "new_field": "default_value",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-04T13:00:00",
        }
    }
    expected = {
        "data": {
            "new_field": "default_value",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-04T13:00:00",
        },
        "migration_status": "non_migrated",
    }
    result = l2_migrator.migrate(entry)
    assert result == expected


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
    expected = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        },
        "migration_status": "error",
    }
    result = l3_nonstrict_migrator.migrate(entry)
    assert result == expected


@pytest.mark.parametrize(
    "migration_version",
    (
        "some string",
        "2022-12-03T23:00:00",
    ),
)
def test_wrong_migration_version(migration_version):
    with pytest.raises(TypeError):
        Migration(
            migration_version,
            "Broken version migration",
            up_function=lambda x: x,
            down_function=lambda x: x,
            downgrade_only=False,
        )


def test_downgrade_only_up_migration(l2_downgrade_migrator):
    entry = {
        "data": {
            "old_field": "value1",
            "schema_version": "1",
        }
    }
    expected = {
        "data": {
            "new_field": "value1",
            "l1_field": "added_in_l1",
            "schema_version": "2022-12-04T13:00:00",
        },
        "migration_status": "migrated_up",
    }
    result = l2_downgrade_migrator.migrate(entry)
    assert result == expected


def test_downgrade_only_down_migration(l2_downgrade_migrator):
    entry = {
        "data": {
            "new_field": "value1",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-05T13:00:00",
        }
    }
    expected = {
        "data": {
            "new_field": "value1",
            "l1_field": "added_in_l1",
            "schema_version": "2022-12-05T12:59:59",
        },
        "migration_status": "migrated_down",
    }
    result = l2_downgrade_migrator.migrate(entry)
    assert result == expected


def test_unknown_entry_migration(l2_downgrade_migrator):
    entry = {
        "data": {
            "new_field": "value1",
            "l1_field": "added_in_l1",
            "l2_field": "added_in_l2",
            "schema_version": "2022-12-06T13:00:00",
        }
    }
    with pytest.raises(UnknownEntryMigration):
        l2_downgrade_migrator.migrate(entry)
