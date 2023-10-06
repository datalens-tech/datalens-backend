from typing import (
    ClassVar,
    Optional,
    Type,
)

import attr

from dl_constants.enums import DataSourceType
from dl_core.connectors.base.data_source_migration import (
    DataSourceMigrationInterface,
    MigrationKeyMappingItem,
    MigrationSpec,
    SpecBasedSourceMigrator,
)
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)


@attr.s(frozen=True)
class SQLTableDSMI(DataSourceMigrationInterface):
    schema_name: Optional[str] = attr.ib(kw_only=True, default=None)
    table_name: Optional[str] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SQLTableWDbDSMI(SQLTableDSMI):
    db_name: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class SQLSubselectDSMI(DataSourceMigrationInterface):
    subsql: Optional[str] = attr.ib(kw_only=True)


class DefaultSQLDataSourceMigrator(SpecBasedSourceMigrator):
    table_source_type: ClassVar[Optional[DataSourceType]] = None
    table_dsrc_spec_cls: ClassVar[Optional[Type[DataSourceSpec]]] = StandardSQLDataSourceSpec
    with_db_name: ClassVar[bool] = False

    subselect_source_type: ClassVar[Optional[DataSourceType]] = None
    subselect_dsrc_spec_cls: ClassVar[Optional[Type[DataSourceSpec]]] = SubselectDataSourceSpec

    default_schema_name: ClassVar[Optional[str]] = None

    def _resolve_schema_name_for_export(
        self,
        source_spec: DataSourceSpec,
        attr_name: str,
    ) -> Optional[str]:
        assert attr_name == "schema_name"
        schema_name: Optional[str] = getattr(source_spec, attr_name, None)
        if schema_name == self.default_schema_name:
            schema_name = None
        return schema_name

    def _resolve_schema_name_for_import(
        self,
        migration_dto: DataSourceMigrationInterface,
        attr_name: str,
    ) -> Optional[str]:
        assert attr_name == "schema_name"
        schema_name: Optional[str] = getattr(migration_dto, attr_name, None)
        if schema_name == self.default_schema_name:
            schema_name = None
        return schema_name

    def get_migration_specs(self) -> list[MigrationSpec]:
        result: list[MigrationSpec] = []
        if self.table_source_type is not None:
            assert self.table_dsrc_spec_cls is not None
            result.append(
                MigrationSpec(
                    source_type=self.table_source_type,
                    dto_cls=SQLTableDSMI,
                    dsrc_spec_cls=self.table_dsrc_spec_cls,
                    migration_mapping_items=(
                        MigrationKeyMappingItem(
                            migration_dto_key="schema_name",
                            source_spec_key="schema_name",
                            custom_export_resolver=self._resolve_schema_name_for_export,
                            custom_import_resolver=self._resolve_schema_name_for_import,
                        ),
                        MigrationKeyMappingItem(migration_dto_key="table_name", source_spec_key="table_name"),
                    ),
                )
            )

            if self.with_db_name:
                result.append(
                    MigrationSpec(
                        source_type=self.table_source_type,
                        dto_cls=SQLTableWDbDSMI,
                        dsrc_spec_cls=self.table_dsrc_spec_cls,
                        migration_mapping_items=(
                            MigrationKeyMappingItem(migration_dto_key="db_name", source_spec_key="db_name"),
                            MigrationKeyMappingItem(
                                migration_dto_key="schema_name",
                                source_spec_key="schema_name",
                                custom_export_resolver=self._resolve_schema_name_for_export,
                                custom_import_resolver=self._resolve_schema_name_for_import,
                            ),
                            MigrationKeyMappingItem(migration_dto_key="table_name", source_spec_key="table_name"),
                        ),
                    )
                )

        if self.subselect_source_type is not None:
            assert self.subselect_dsrc_spec_cls is not None
            result.append(
                MigrationSpec(
                    source_type=self.subselect_source_type,
                    dto_cls=SQLSubselectDSMI,
                    dsrc_spec_cls=self.subselect_dsrc_spec_cls,
                    migration_mapping_items=(
                        MigrationKeyMappingItem(migration_dto_key="subsql", source_spec_key="subsql"),
                    ),
                )
            )

        return result
