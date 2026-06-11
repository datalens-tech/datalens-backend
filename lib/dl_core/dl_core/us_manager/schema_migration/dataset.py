from dl_core.us_manager.schema_migration.base import (
    BaseEntrySchemaMigration,
    Migration,
)
from dl_core.us_manager.schema_migration.migrations.resolve_group_slugs import RESOLVE_GROUP_SLUGS_MIGRATION


class DatasetSchemaMigration(BaseEntrySchemaMigration):
    @property
    def migrations(self) -> list[Migration]:
        migrations = [RESOLVE_GROUP_SLUGS_MIGRATION]
        migrations.extend(super().migrations)
        return migrations
