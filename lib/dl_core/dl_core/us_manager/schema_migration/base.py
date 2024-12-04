from copy import deepcopy
import logging
from typing import (
    Callable,
    ClassVar,
)

import attr


LOGGER = logging.getLogger(__name__)


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
