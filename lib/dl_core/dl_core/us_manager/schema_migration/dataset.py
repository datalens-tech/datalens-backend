from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)


class DatasetSchemaMigration(BaseEntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = []
        migrations.extend(super().migrations)
        return migrations
