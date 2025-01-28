from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
)

import attr
from typing_extensions import Self

from dl_app_tools.profiling_base import (
    generic_profiler,
    generic_profiler_async,
)
from dl_core.exc import UnknownEntryMigration


if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


def isoformat_validator(instance: Any, attribute: attr.Attribute, value: str) -> None:
    datetime.fromisoformat(value)


class MigrationFunction(Protocol):
    def __call__(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        ...


class AwaitMigrationFunction(Protocol):
    async def __call__(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        ...


@attr.s
class Migration:
    """
    Note:
        Migrations should first be deployed with the `downgrade_only` flag set to True.
        This ensures that only downgrade operations are allowed initially.
        After the release, the `downgrade_only` flag should be removed to enable the migration upwards.
    """

    version: str = attr.ib(validator=isoformat_validator)
    name: str = attr.ib()
    up_function: MigrationFunction = attr.ib()
    down_function: MigrationFunction = attr.ib()
    await_up_function: AwaitMigrationFunction | None = attr.ib(default=None)
    await_down_function: AwaitMigrationFunction | None = attr.ib(default=None)
    downgrade_only: bool = attr.ib(default=True)
    id: int = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.id = int(datetime.fromisoformat(self.version).timestamp())

    def __lt__(self, other: Self) -> bool:
        return self.id < other.id

    @property
    def downgrade_version(self) -> str:
        return datetime.fromtimestamp(self.id - 1).isoformat()

    def migrate_up(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        if not self.downgrade_only:
            entry = self.up_function(entry, services_registry=services_registry)
            entry["data"]["schema_version"] = self.version
        return entry

    def migrate_down(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        entry = self.down_function(entry, services_registry=services_registry)
        entry["data"]["schema_version"] = self.downgrade_version
        return entry

    async def migrate_up_async(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        if not self.downgrade_only and self.await_up_function is not None:
            entry = await self.await_up_function(entry, services_registry=services_registry)
            entry["data"]["schema_version"] = self.version
            return entry
        return self.up_function(entry, services_registry=services_registry)

    async def migrate_down_async(self, entry: dict, services_registry: ServicesRegistry | None = None) -> dict:
        if self.await_down_function is not None:
            entry = await self.await_down_function(entry, services_registry=services_registry)
            entry["data"]["schema_version"] = self.downgrade_version
            return entry
        return self.down_function(entry, services_registry=services_registry)


@attr.s
class BaseEntrySchemaMigration:
    services_registry: ServicesRegistry | None = attr.ib(default=None)
    strict_migration: bool = attr.ib(default=False)

    @property
    def migrations(self) -> list[Migration]:
        return []

    @property
    def sorted_migrations(self) -> list[Migration]:
        return sorted(self.migrations)

    @property
    def migration_ids(self) -> set[int]:
        return {migration.id for migration in self.migrations}

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

    @generic_profiler("migrate_entry")
    def migrate(self, entry: dict) -> dict:
        entry_copy = deepcopy(entry)
        seen_versions = set()

        try:
            entry_schema_id = self._get_entry_schema_id(entry_copy)
            if entry_schema_id != 1 and entry_schema_id not in self.migration_ids:
                raise UnknownEntryMigration(
                    f"Unknown entry version: {datetime.fromtimestamp(entry_schema_id).isoformat()}"
                )
            for migration in self.sorted_migrations:
                if migration.id <= entry_schema_id or migration.downgrade_only:
                    continue
                if migration.version in seen_versions:
                    raise ValueError(f"Double migration detected for migration version: {migration.version}")
                LOGGER.info(f"Apply migration ver={migration.version}, {migration.name}")
                entry_copy = migration.migrate_up(entry_copy, self.services_registry)
                seen_versions.add(migration.version)
            # Rollback last migration if it needed
            last_migration = self.sorted_migrations[-1]
            if last_migration.id == entry_schema_id and last_migration.downgrade_only:
                LOGGER.info(f"Rollback migration ver={last_migration.version}, {last_migration.name}")
                entry_copy = last_migration.migrate_down(entry_copy, self.services_registry)
            return entry_copy
        except UnknownEntryMigration:
            raise
        except Exception as exc:
            if self.strict_migration:
                raise exc
            LOGGER.warning("Entry migration failed", exc_info=True)
            return deepcopy(entry)

    @generic_profiler_async("migrate_entry")  # type: ignore  # TODO: fix
    async def migrate_async(self, entry: dict) -> dict:
        entry_copy = deepcopy(entry)
        seen_versions = set()

        try:
            entry_schema_id = self._get_entry_schema_id(entry_copy)
            if entry_schema_id != 1 and entry_schema_id not in self.migration_ids:
                raise UnknownEntryMigration(
                    f"Unknown entry version: {datetime.fromtimestamp(entry_schema_id).isoformat()}"
                )
            for migration in self.sorted_migrations:
                if migration.id <= entry_schema_id or migration.downgrade_only:
                    continue
                if migration.version in seen_versions:
                    raise ValueError(f"Double migration detected for migration version: {migration.version}")
                LOGGER.info(f"Apply migration ver={migration.version}, {migration.name}")
                entry_copy = await migration.migrate_up_async(entry_copy, self.services_registry)
                seen_versions.add(migration.version)
            # Rollback last migration if it needed
            last_migration = self.sorted_migrations[-1]
            if last_migration.id == entry_schema_id and last_migration.downgrade_only:
                LOGGER.info(f"Rollback migration ver={last_migration.version}, {last_migration.name}")
                entry_copy = await last_migration.migrate_down_async(entry_copy, self.services_registry)
            return entry_copy
        except UnknownEntryMigration:
            raise
        except Exception as exc:
            if self.strict_migration:
                raise exc
            LOGGER.warning("Entry migration failed", exc_info=True)
            return deepcopy(entry)
