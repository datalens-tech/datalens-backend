from __future__ import annotations

from typing import TYPE_CHECKING

from dl_core.us_connection import get_schema_migration_cls
from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration
from dl_core.us_manager.schema_migration.dataset import DatasetSchemaMigration
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase


if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry


class DummyEntrySchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
        service_registry: ServicesRegistry | None = None,
    ) -> BaseEntrySchemaMigration:
        return BaseEntrySchemaMigration()


class DefaultEntrySchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
        service_registry: ServicesRegistry | None = None,
    ) -> BaseEntrySchemaMigration:
        if entry_scope == "dataset":
            return DatasetSchemaMigration(services_registry=service_registry)
        elif entry_scope == "connection":
            schema_migration_cls = get_schema_migration_cls(conn_type_name=entry_type)
            return schema_migration_cls(services_registry=service_registry)
        else:
            return BaseEntrySchemaMigration(services_registry=service_registry)
