from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration


if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


class EntrySchemaMigrationFactoryBase(abc.ABC):
    @abc.abstractmethod
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
        service_registry: ServicesRegistry | None = None,
    ) -> BaseEntrySchemaMigration:
        pass
