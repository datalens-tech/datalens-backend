import abc
from typing import Any, Callable, Optional, Sequence, Type

import attr

from bi_constants.enums import ConnectionType, CreateDSFrom

import bi_core.exc as exc
from bi_core.base_models import ConnectionRef
from bi_core.data_source_spec.base import DataSourceSpec


@attr.s(frozen=True)
class DataSourceMigrationInterface:
    """Base class for all data source migration interfaces"""


class DataSourceMigrator(abc.ABC):
    @abc.abstractmethod
    def export_migration_dtos(
            self, data_source_spec: DataSourceSpec,
    ) -> Sequence[DataSourceMigrationInterface]:
        raise NotImplementedError

    def import_migration_dtos(
            self, migration_dtos: Sequence[DataSourceMigrationInterface],
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:
        chosen_dto = self._choose_migration_dto(migration_dtos=migration_dtos)
        return self._load_migration_dto(migration_dto=chosen_dto, connection_ref=connection_ref)

    @abc.abstractmethod
    def _choose_migration_dto(
            self, migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> DataSourceMigrationInterface:
        raise NotImplementedError

    @abc.abstractmethod
    def _load_migration_dto(
            self, migration_dto: DataSourceMigrationInterface,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:
        raise NotImplementedError


class DefaultDataSourceMigrator(DataSourceMigrator):
    def export_migration_dtos(
            self, data_source_spec: DataSourceSpec,
    ) -> Sequence[DataSourceMigrationInterface]:
        return ()

    def _choose_migration_dto(
            self, migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> DataSourceMigrationInterface:
        raise exc.DataSourceMigrationImpossible()

    def _load_migration_dto(
            self, migration_dto: DataSourceMigrationInterface,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:
        raise NotImplementedError


@attr.s(frozen=True)
class MigrationKeyMappingItem:
    migration_dto_key: str = attr.ib(kw_only=True)
    source_spec_key: str = attr.ib(kw_only=True)
    custom_export_resolver: Optional[Callable[[DataSourceSpec, str], Any]] = \
        attr.ib(kw_only=True, default=None)
    custom_import_resolver: Optional[Callable[[DataSourceMigrationInterface, str], Any]] = \
        attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class MigrationSpec:
    source_type: CreateDSFrom = attr.ib(kw_only=True)
    dto_cls: Type[DataSourceMigrationInterface] = attr.ib(kw_only=True)
    dsrc_spec_cls: Type[DataSourceSpec] = attr.ib(kw_only=True)
    migration_mapping_items: tuple[MigrationKeyMappingItem, ...] = attr.ib(kw_only=True)


class SpecBasedSourceMigrator(DataSourceMigrator):
    def get_migration_specs(self) -> list[MigrationSpec]:
        return []

    def _export_for_migration_spec(
            self, data_source_spec: DataSourceSpec, migration_spec: MigrationSpec,
    ) -> DataSourceMigrationInterface:
        assert data_source_spec.source_type == migration_spec.source_type
        params: dict[str, Any] = {}

        for mapping_item in migration_spec.migration_mapping_items:
            if mapping_item.custom_export_resolver is not None:
                params[mapping_item.migration_dto_key] = mapping_item.custom_export_resolver(
                    data_source_spec, mapping_item.source_spec_key)
            elif hasattr(data_source_spec, mapping_item.source_spec_key):
                params[mapping_item.migration_dto_key] = getattr(data_source_spec, mapping_item.source_spec_key)

        return migration_spec.dto_cls(**params)  # type: ignore

    def export_migration_dtos(
            self, data_source_spec: DataSourceSpec,
    ) -> Sequence[DataSourceMigrationInterface]:
        result: list[DataSourceMigrationInterface] = []
        for migration_spec in self.get_migration_specs():
            if migration_spec.source_type == data_source_spec.source_type:
                result.append(self._export_for_migration_spec(
                    data_source_spec=data_source_spec,
                    migration_spec=migration_spec,
                ))

        return result

    def _choose_migration_dto(
            self, migration_dtos: Sequence[DataSourceMigrationInterface],
    ) -> DataSourceMigrationInterface:
        migration_specs = self.get_migration_specs()
        for dto in migration_dtos:
            for migration_spec in migration_specs:
                if type(dto) is migration_spec.dto_cls:
                    return dto

        raise exc.DataSourceMigrationImpossible()

    def _load_migration_dto(
            self, migration_dto: DataSourceMigrationInterface,
            connection_ref: ConnectionRef,
    ) -> DataSourceSpec:

        for migration_spec in self.get_migration_specs():
            if type(migration_dto) is not migration_spec.dto_cls:
                continue

            params: dict[str, Any] = {'source_type': migration_spec.source_type, 'connection_ref': connection_ref}
            for mapping_item in migration_spec.migration_mapping_items:
                if mapping_item.custom_import_resolver is not None:
                    params[mapping_item.source_spec_key] = mapping_item.custom_import_resolver(
                        migration_dto, mapping_item.migration_dto_key)
                elif mapping_item.source_spec_key in migration_spec.dsrc_spec_cls.get_public_fields():
                    params[mapping_item.source_spec_key] = getattr(migration_dto, mapping_item.migration_dto_key)

            params = {key: value for key, value in params.items() if value is not None}
            dsrc_spec = migration_spec.dsrc_spec_cls(**params)
            return dsrc_spec

        raise TypeError(f'Invalid dto class: {type(migration_dto)}')


_SOURCE_MIGRATORS: dict[ConnectionType, Type[DataSourceMigrator]] = {
    ConnectionType.unknown: DefaultDataSourceMigrator,
}


def register_data_source_migrator(conn_type: ConnectionType, migrator_cls: Type[DataSourceMigrator]) -> None:
    try:
        assert _SOURCE_MIGRATORS[conn_type] is migrator_cls
    except KeyError:
        _SOURCE_MIGRATORS[conn_type] = migrator_cls


def get_data_source_migrator(conn_type: ConnectionType) -> DataSourceMigrator:
    return _SOURCE_MIGRATORS[conn_type]()
