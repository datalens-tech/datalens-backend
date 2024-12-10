from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

import attr
from typing_extensions import Self

from dl_app_tools.profiling_base import generic_profiler


if TYPE_CHECKING:
    from dl_api_commons.base_models import RequestContextInfo
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


def isoformat_validator(instance: Any, attribute: attr.Attribute, value: str) -> None:
    datetime.fromisoformat(value)


@attr.s
class Migration:
    version: str = attr.ib(validator=isoformat_validator)
    name: str = attr.ib()
    function: Callable[[dict], dict] = attr.ib()
    id: int = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.id = int(datetime.fromisoformat(self.version).timestamp())

    def __lt__(self, other: Self) -> bool:
        return self.id < other.id

    def migrate(self, entry: dict) -> dict:
        entry = self.function(entry)
        entry["data"]["schema_version"] = self.version
        return entry


@attr.s
class BaseEntrySchemaMigration:
    bi_context: RequestContextInfo | None = attr.ib(default=None)
    services_registry: ServicesRegistry | None = attr.ib(default=None)
    strict_migration: bool = attr.ib(default=False)

    @property
    def migrations(self) -> list[Migration]:
        return []

    @property
    def sorted_migrations(self) -> list[Migration]:
        return sorted(self.migrations)

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
        entry_schema_id = self._get_entry_schema_id(entry)

        for migration in self.sorted_migrations:
            if migration.id <= entry_schema_id:
                continue
            if migration.version in seen_versions:
                raise ValueError(f"Double migration detected for migration version: {migration.version}")
            seen_versions.add(migration.version)
            LOGGER.info(f"Apply migration ver={migration.version}, {migration.name}")
            entry = migration.migrate(entry)
        return entry

    @generic_profiler("migrate_entry")
    def migrate(self, entry: dict) -> dict:
        entry_copy = deepcopy(entry)

        try:
            return self._migrate(entry_copy)
        except Exception as exc:
            if self.strict_migration:
                raise exc
            LOGGER.warning("Entry migration failed", exc_info=True)
            return deepcopy(entry)
