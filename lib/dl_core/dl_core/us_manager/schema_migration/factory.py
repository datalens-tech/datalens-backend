from __future__ import annotations

from dl_core.us_connection import get_schema_migration_cls
from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration
from dl_core.us_manager.schema_migration.dataset import DatasetSchemaMigration
from dl_core.us_manager.schema_migration.factory_base import EntrySchemaMigrationFactoryBase


class DummyEntrySchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
    ) -> BaseEntrySchemaMigration:
        return BaseEntrySchemaMigration()


class DefaultEntrySchemaMigrationFactory(EntrySchemaMigrationFactoryBase):
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
    ) -> BaseEntrySchemaMigration:
        if entry_scope == "dataset":
            return DatasetSchemaMigration()
        elif entry_scope == "connection":
            schema_migration_cls = get_schema_migration_cls(conn_type_name=entry_type)
            return schema_migration_cls()
        else:
            return BaseEntrySchemaMigration()
