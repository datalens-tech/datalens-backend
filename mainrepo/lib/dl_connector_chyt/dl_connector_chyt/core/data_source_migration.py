from typing import (
    ClassVar,
    Optional,
    Type,
)

import attr

from dl_connector_chyt.core.constants import (
    SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
)
from dl_connector_chyt.core.data_source_spec import (
    CHYTSubselectDataSourceSpec,
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
)
from dl_constants.enums import DataSourceType
from dl_core.connectors.base.data_source_migration import (
    DataSourceMigrationInterface,
    MigrationKeyMappingItem,
    MigrationSpec,
)
from dl_core.connectors.sql_base.data_source_migration import DefaultSQLDataSourceMigrator
from dl_core.data_source_spec.base import DataSourceSpec


@attr.s(frozen=True)
class CHYTTableListDSMI(DataSourceMigrationInterface):
    table_names: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class CHYTTableRangeDSMI(DataSourceMigrationInterface):
    directory_path: Optional[str] = attr.ib(kw_only=True, default=None)
    range_from: Optional[str] = attr.ib(kw_only=True, default=None)
    range_to: Optional[str] = attr.ib(kw_only=True, default=None)


class BaseCHYTDataSourceMigrator(DefaultSQLDataSourceMigrator):
    table_list_source_type: ClassVar[Optional[DataSourceType]]
    table_list_dsrc_spec_cls: ClassVar[Optional[Type[DataSourceSpec]]]

    table_range_source_type: ClassVar[Optional[DataSourceType]]
    table_range_dsrc_spec_cls: ClassVar[Optional[Type[DataSourceSpec]]]

    def get_migration_specs(self) -> list[MigrationSpec]:
        result = super().get_migration_specs()

        if self.table_list_source_type is not None:
            assert self.table_list_dsrc_spec_cls is not None
            result.append(
                MigrationSpec(
                    source_type=self.table_list_source_type,
                    dto_cls=CHYTTableListDSMI,
                    dsrc_spec_cls=self.table_list_dsrc_spec_cls,
                    migration_mapping_items=(
                        MigrationKeyMappingItem(migration_dto_key="table_names", source_spec_key="table_names"),
                    ),
                )
            )

        if self.table_range_source_type is not None:
            assert self.table_range_dsrc_spec_cls is not None
            result.append(
                MigrationSpec(
                    source_type=self.table_range_source_type,
                    dto_cls=CHYTTableRangeDSMI,
                    dsrc_spec_cls=self.table_range_dsrc_spec_cls,
                    migration_mapping_items=(
                        MigrationKeyMappingItem(migration_dto_key="directory_path", source_spec_key="directory_path"),
                        MigrationKeyMappingItem(migration_dto_key="range_from", source_spec_key="range_from"),
                        MigrationKeyMappingItem(migration_dto_key="range_to", source_spec_key="range_to"),
                    ),
                )
            )

        return result


class CHYTDataSourceMigrator(BaseCHYTDataSourceMigrator):
    table_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE
    table_dsrc_spec_cls = CHYTTableDataSourceSpec

    subselect_source_type = SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT
    subselect_dsrc_spec_cls = CHYTSubselectDataSourceSpec

    table_list_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST
    table_list_dsrc_spec_cls = CHYTTableListDataSourceSpec

    table_range_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE
    table_range_dsrc_spec_cls = CHYTTableRangeDataSourceSpec
