from copy import deepcopy
from datetime import datetime
import logging
from typing import (
    Callable,
    ClassVar,
)

import attr


LOGGER = logging.getLogger(__name__)


@attr.s
class Migration:
    version: str = attr.ib()
    name: str = attr.ib()
    function: Callable[[dict], dict] = attr.ib()
    id: int = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.id = int(datetime.fromisoformat(self.version).timestamp())

    def migrate(self, entry: dict) -> dict:
        entry = self.function(entry)
        entry["data"]["schema_version"] = self.version
        return entry


@attr.s
class BaseEntrySchemaMigration:
    _MIGRATIONS: ClassVar[list[Migration]] = attr.ib(init=False, factory=list)
    strict_migration: bool = attr.ib(default=False)

    @classmethod
    def _collect_migrations(cls) -> list[Migration]:
        migrations: list[Migration] = []
        for _cls in cls.__mro__:
            if issubclass(_cls, BaseEntrySchemaMigration) and hasattr(_cls, "_MIGRATIONS"):
                migrations.extend(_cls._MIGRATIONS)
        migrations.sort(key=lambda item: item.id)
        return migrations

    @staticmethod
    def _get_entry_schema_id(entry: dict) -> int:
        entry_data = entry.get("data")
        if not isinstance(entry_data, dict):
            raise ValueError(f"Invalid entry: 'data' should be a dict, got {type(entry_data).__name__}")

        schema_version = entry_data.get("schema_version", "")
        entry_schema_id = 1
        if schema_version != "1":
            entry_schema_id = int(datetime.fromisoformat(schema_version).timestamp())
        return entry_schema_id

    def _migrate(self, entry: dict) -> dict:
        seen_versions = set()
        migrations = self._collect_migrations()
        entry_schema_id = self._get_entry_schema_id(entry)

        for migration in migrations:
            if migration.id <= entry_schema_id:
                continue
            if migration.version in seen_versions:
                raise ValueError(f"Double migration detected for migration version: {migration.version}")
            seen_versions.add(migration.version)
            LOGGER.info(f"Apply migration ver={migration.version}, {migration.name}")
            entry = migration.migrate(entry)
        return entry

    def migrate(self, entry: dict) -> dict:
        original_entry = deepcopy(entry)

        try:
            return self._migrate(entry)
        except Exception as exc:
            if self.strict_migration:
                raise exc
            LOGGER.warning("Entry migration failed", exc_info=True)
            return original_entry
