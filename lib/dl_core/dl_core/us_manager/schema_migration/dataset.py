from __future__ import annotations

import datetime
import typing

from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)


if typing.TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


class DatasetSchemaMigration(BaseEntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [
            Migration(
                datetime.datetime(2025, 4, 22),
                "BI-6217 - Rename AllParameterValueConstraint->NullParameterValueConstraint",
                up_function=DatasetSchemaMigration._bi_6217_up,
                down_function=DatasetSchemaMigration._bi_6217_down,
                downgrade_only=False,
            ),
        ]
        migrations.extend(super().migrations)
        return migrations

    @staticmethod
    def _bi_6217_up(entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        for field in entry["data"]["result_schema"]:
            if field["calc_mode"] != "parameter":
                continue

            if not isinstance(field["value_constraint"], dict):
                continue

            if field["value_constraint"].get("type") == "all":
                field["value_constraint"]["type"] = "null"

        return entry

    @staticmethod
    def _bi_6217_down(entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        return entry
