import pytest

from dl_core.us_manager.schema_migration.dataset import DatasetSchemaMigration


@pytest.fixture
def migrator() -> DatasetSchemaMigration:
    return DatasetSchemaMigration(strict_migration=True)


def test_bi_6217_up(migrator: DatasetSchemaMigration):
    entry = {
        "data": {
            "result_schema": [
                {"calc_mode": "other", "value_constraint": None},
                {"calc_mode": "parameter", "value_constraint": None},
                {"calc_mode": "parameter", "value_constraint": {"type": "all"}},
                {"calc_mode": "parameter", "value_constraint": {"type": "other"}},
            ],
            "schema_version": "1",
        },
    }

    expected = {
        "data": {
            "result_schema": [
                {"calc_mode": "other", "value_constraint": None},
                {"calc_mode": "parameter", "value_constraint": None},
                {"calc_mode": "parameter", "value_constraint": {"type": "null"}},
                {"calc_mode": "parameter", "value_constraint": {"type": "other"}},
            ],
            "schema_version": "2025-04-22T00:00:00",
        },
        "migration_status": "migrated_up",
    }

    result = migrator.migrate(entry)
    assert result == expected
