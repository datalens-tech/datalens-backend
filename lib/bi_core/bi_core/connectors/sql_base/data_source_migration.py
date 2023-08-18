from typing import Any, ClassVar, Optional, Sequence

import attr

from bi_constants.enums import CreateDSFrom

from bi_core.base_models import ConnectionRef
from bi_core.data_source_spec.base import DataSourceSpec
from bi_core.data_source_spec.sql import (
    DbSQLDataSourceSpec, SchemaSQLDataSourceSpec, TableSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from bi_core.data_source_spec.type_mapping import get_data_source_spec_class
from bi_core.connectors.base.data_source_migration import (
    DataSourceMigrationInterface, DataSourceMigrator,
)


@attr.s(frozen=True)
class SQLTableDSMI(DataSourceMigrationInterface):
    db_name: Optional[str] = attr.ib(kw_only=True, default=None)
    schema_name: Optional[str] = attr.ib(kw_only=True, default=None)
    table_name: Optional[str] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SQLSubselectDSMI(DataSourceMigrationInterface):
    subselect: str = attr.ib(kw_only=True)


class DefaultSQLDataSourceMigrator(DataSourceMigrator):
    table_source_type: ClassVar[Optional[CreateDSFrom]] = None
    subselect_source_type: ClassVar[Optional[CreateDSFrom]] = None
    default_schema_name: ClassVar[Optional[str]] = None

    def export_migration_dtos(
            self, data_source_spec: DataSourceSpec,
    ) -> Sequence[DataSourceMigrationInterface]:
        params: dict[str, Any] = {}
        if isinstance(data_source_spec, DbSQLDataSourceSpec):
            params['db_name'] = data_source_spec.db_name
        if isinstance(data_source_spec, SchemaSQLDataSourceSpec):
            if data_source_spec.schema_name != self.default_schema_name:
                params['schema_name'] = data_source_spec.schema_name
        if isinstance(data_source_spec, TableSQLDataSourceSpec):
            params['table_name'] = data_source_spec.table_name

        return [SQLTableDSMI(**params)]

    def _choose_migration_dto(
            self, migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> DataSourceMigrationInterface:
        return next(
            dto for dto in migration_dtos
            if (
                isinstance(dto, SQLTableDSMI) and self.table_source_type is not None
                or isinstance(dto, SQLSubselectDSMI) and self.subselect_source_type is not None
            )
        )

    def _load_migration_dto_table(
            self, migration_dto: SQLTableDSMI, source_type: CreateDSFrom,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:
        params: dict[str, Any] = {'source_type': source_type, 'connection_ref': connection_ref}
        table_dsrc_cls = get_data_source_spec_class(source_type)
        if issubclass(table_dsrc_cls, DbSQLDataSourceSpec):
            params['db_name'] = migration_dto.db_name
        if issubclass(table_dsrc_cls, SchemaSQLDataSourceSpec):
            params['schema_name'] = migration_dto.schema_name or self.default_schema_name
        assert issubclass(table_dsrc_cls, TableSQLDataSourceSpec)
        params['table_name'] = migration_dto.table_name
        dsrc_spec = table_dsrc_cls(**params)
        assert isinstance(dsrc_spec, TableSQLDataSourceSpec)
        return dsrc_spec

    def _load_migration_dto_subselect(
            self, migration_dto: SQLSubselectDSMI, source_type: CreateDSFrom,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:
        params: dict[str, Any] = {'source_type': source_type, 'connection_ref': connection_ref}
        table_dsrc_cls = get_data_source_spec_class(source_type)
        assert issubclass(table_dsrc_cls, DbSQLDataSourceSpec)
        params['subselect'] = migration_dto.subselect
        dsrc_spec = table_dsrc_cls(**params)
        assert isinstance(dsrc_spec, SubselectDataSourceSpec)
        return dsrc_spec

    def _load_migration_dto(
            self, migration_dto: DataSourceMigrationInterface,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:

        if isinstance(migration_dto, SQLTableDSMI) and self.table_source_type is not None:
            return self._load_migration_dto_table(
                migration_dto, source_type=self.table_source_type,
                connection_ref=connection_ref,
            )

        elif isinstance(migration_dto, SQLSubselectDSMI) and self.subselect_source_type is not None:
            return self._load_migration_dto_subselect(
                migration_dto, source_type=self.subselect_source_type,
                connection_ref=connection_ref,
            )

        raise RuntimeError(f'Invalid dto class: {type(migration_dto)}')
