from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)


class ConnectionSchemaMigration(BaseEntrySchemaMigration):
    ...


class DefaultConnectionSchemaMigration(ConnectionSchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = []
        migrations.extend(super().migrations)
        return migrations
