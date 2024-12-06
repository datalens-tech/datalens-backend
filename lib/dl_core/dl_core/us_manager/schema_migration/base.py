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
    index: int = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.index = int(datetime.fromisoformat(self.version).timestamp())


@attr.s
class BaseLinearEntrySchemaMigration:
    strict_migration: bool = attr.ib(default=False)
    _MIGRATIONS: ClassVar[list[Migration]] = attr.ib(init=False, factory=list)

    def _collect_migrations(self) -> list[Migration]:
        migrations: list[Migration] = []
        for cls in self.__class__.__mro__:
            if issubclass(cls, BaseLinearEntrySchemaMigration) and hasattr(cls, "_MIGRATIONS"):
                migrations.extend(cls._MIGRATIONS)
        migrations.sort(key=lambda item: item.index)
        return migrations

    def _migrate(self, entry: dict) -> dict:
        seen_versions = set()
        migrations = self._collect_migrations()
        entry_data = entry.get("data")

        if not isinstance(entry_data, dict):
            raise ValueError(f"Invalid entry: 'data' should be a dict, got {type(entry_data).__name__}")

        schema_version = str(entry_data.get("schema_version", ""))
        schema_index = 1
        if schema_version != "1":
            schema_index = int(datetime.fromisoformat(schema_version).timestamp())

        for migration in migrations:
            if migration.index <= schema_index:
                continue
            if migration.version in seen_versions:
                raise ValueError(f"Double migration detected for migration version: {migration.version}")
            seen_versions.add(migration.version)
            LOGGER.info(f"Apply migration ver={migration.version}, {migration.name}")
            entry = migration.function(entry)
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


@attr.s
class BaseEntrySchemaMigration:
    strict_migration: bool = attr.ib(default=False)
    _MIGRATIONS: ClassVar[dict[str, Callable[[dict], dict]]] = attr.ib(init=False, factory=dict)

    def _migrate(self, entry: dict) -> dict:
        seen_versions = set()

        entry_data = entry.get("data")
        if not isinstance(entry_data, dict):
            raise ValueError(f"Invalid entry: 'data' should be a dict, got {type(entry_data).__name__}")

        schema_version = str(entry_data.get("schema_version", ""))

        while schema_version in self.__class__._MIGRATIONS:
            if schema_version in seen_versions:
                raise ValueError(f"Cyclic migration detected for schema_version: {schema_version}")
            seen_versions.add(schema_version)

            migration_func = self.__class__._MIGRATIONS[schema_version]
            entry = migration_func(entry)
            schema_version = str(entry["data"].get("schema_version", ""))

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
