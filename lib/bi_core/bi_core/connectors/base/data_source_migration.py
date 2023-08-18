import abc
from typing import Sequence, Type

import attr

from bi_constants.enums import ConnectionType

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


_SOURCE_MIGRATORS: dict[ConnectionType, Type[DataSourceMigrator]] = {}


def register_data_source_migrator(conn_type: ConnectionType, migrator_cls: Type[DataSourceMigrator]) -> None:
    try:
        assert _SOURCE_MIGRATORS[conn_type] is migrator_cls
    except KeyError:
        _SOURCE_MIGRATORS[conn_type] = migrator_cls


def get_data_source_migrator(conn_type: ConnectionType) -> DataSourceMigrator:
    return _SOURCE_MIGRATORS[conn_type]()
