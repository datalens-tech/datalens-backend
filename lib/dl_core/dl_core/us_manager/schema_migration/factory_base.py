import abc

from dl_core.us_manager.schema_migration.base import BaseEntrySchemaMigration


class EntrySchemaMigrationFactoryBase(abc.ABC):
    @abc.abstractmethod
    def get_schema_migration(
        self,
        entry_scope: str,
        entry_type: str,
    ) -> BaseEntrySchemaMigration:
        pass
