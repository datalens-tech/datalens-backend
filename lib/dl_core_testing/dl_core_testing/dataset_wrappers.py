from __future__ import annotations

from itertools import chain
from typing import (
    Any,
    Callable,
    FrozenSet,
    Iterable,
    List,
    Optional,
)

import attr

from dl_constants.enums import (
    DataSourceCreatedVia,
    DataSourceRole,
    DataSourceType,
    JoinType,
    ManagedBy,
)
from dl_core.base_models import (
    DefaultWhereClause,
    ObligatoryFilter,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.editor import DatasetComponentEditor
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.data_source.base import DataSource
from dl_core.data_source.collection import (
    DataSourceCollectionBase,
    DataSourceCollectionFactory,
)
from dl_core.data_source.sql import BaseSQLDataSource
from dl_core.data_source_spec.collection import DataSourceCollectionSpecBase
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core.db.elements import (
    IndexInfo,
    SchemaColumn,
)
from dl_core.fields import (
    BIField,
    ResultSchema,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    SourceAvatar,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.us_manager.us_manager import USManagerBase


@attr.s
class DatasetTestWrapper:
    """
    A helper class for tests.
    It consolidates the interfaces of various dataset wrappers
    and factories.

    Fo use in tests only!
    """

    _dataset: Dataset = attr.ib()
    _us_manager: Optional[USManagerBase] = attr.ib(kw_only=True, default=None)  # FIXME: Legacy; remove
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(init=False)
    _ds_capabilities: DatasetCapabilities = attr.ib(init=False)

    @_us_entry_buffer.default
    def _make_us_entry_buffer(self) -> USEntryBuffer:
        # TODO: Remove with self._us_manager
        assert self._us_manager is not None
        return self._us_manager.get_entry_buffer()

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    @_dsrc_coll_factory.default
    def _make_dsrc_coll_factory(self) -> DataSourceCollectionFactory:
        return DataSourceCollectionFactory(us_entry_buffer=self._us_entry_buffer)

    @_ds_capabilities.default
    def _make_ds_capabilities(self) -> DatasetCapabilities:
        return DatasetCapabilities(dataset=self._dataset, dsrc_coll_factory=self._dsrc_coll_factory)

    @property
    def dsrc_coll_factory(self) -> DataSourceCollectionFactory:
        return self._dsrc_coll_factory

    def get_root_avatar_opt(self) -> Optional[SourceAvatar]:
        return self._ds_accessor.get_root_avatar_opt()

    def get_root_avatar_strict(self) -> SourceAvatar:
        return self._ds_accessor.get_root_avatar_strict()

    def get_avatar_list(self, source_id: Optional[str] = None) -> list[SourceAvatar]:
        return self._ds_accessor.get_avatar_list(source_id=source_id)

    def get_avatar_opt(self, avatar_id: str) -> Optional[SourceAvatar]:
        return self._ds_accessor.get_avatar_opt(avatar_id=avatar_id)

    def get_avatar_strict(self, avatar_id: str) -> SourceAvatar:
        return self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)

    def get_data_source_coll_spec_opt(self, source_id: str) -> Optional[DataSourceCollectionSpecBase]:
        return self._ds_accessor.get_data_source_coll_spec_opt(source_id=source_id)

    def get_data_source_coll_spec_strict(self, source_id: str) -> DataSourceCollectionSpecBase:
        return self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)

    def get_data_source_coll_list(self) -> list[DataSourceCollectionBase]:
        return [self.get_data_source_coll_strict(source_id=source_id) for source_id in self.get_data_source_id_list()]

    def get_data_source_coll_opt(self, source_id: str) -> Optional[DataSourceCollectionBase]:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        return dsrc_coll

    def get_data_source_coll_strict(self, source_id: str) -> DataSourceCollectionBase:
        dsrc_coll = self.get_data_source_coll_opt(source_id=source_id)
        assert dsrc_coll is not None
        return dsrc_coll

    def get_data_source_id_list(self) -> list[str]:
        return self._ds_accessor.get_data_source_id_list()

    def get_data_source_list(self, role: DataSourceRole) -> list[DataSource]:
        return [
            self.get_data_source_strict(source_id=source_id, role=role) for source_id in self.get_data_source_id_list()
        ]

    def get_data_source_opt(self, source_id: str, role: DataSourceRole) -> Optional[DataSource]:
        assert role is not None
        dsrc_coll = self.get_data_source_coll_strict(source_id=source_id)
        dsrc = dsrc_coll.get_opt(role=role)
        return dsrc

    def get_data_source_strict(self, source_id: str, role: DataSourceRole) -> DataSource:
        assert role is not None
        dsrc_coll = self.get_data_source_coll_strict(source_id=source_id)
        dsrc = dsrc_coll.get_strict(role=role)
        return dsrc

    def get_sql_data_source_strict(self, source_id: str, role: DataSourceRole) -> BaseSQLDataSource:
        dsrc = self.get_data_source_strict(source_id=source_id, role=role)
        assert isinstance(dsrc, BaseSQLDataSource)
        return dsrc

    def get_avatar_relation_list(
        self, left_avatar_id: Optional[str] = None, right_avatar_id: Optional[str] = None
    ) -> list[AvatarRelation]:
        return self._ds_accessor.get_avatar_relation_list(
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
        )

    def get_avatar_relation_strict(
        self,
        relation_id: Optional[str] = None,
        left_avatar_id: Optional[str] = None,
        right_avatar_id: Optional[str] = None,
    ) -> AvatarRelation:
        return self._ds_accessor.get_avatar_relation_strict(
            relation_id=relation_id,
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
        )

    def get_obligatory_filter_list(self) -> list[ObligatoryFilter]:
        return self._ds_accessor.get_obligatory_filter_list()

    def get_obligatory_filter_opt(
        self,
        obfilter_id: Optional[str] = None,
        field_guid: Optional[str] = None,
    ) -> Optional[ObligatoryFilter]:
        return self._ds_accessor.get_obligatory_filter_opt(obfilter_id=obfilter_id, field_guid=field_guid)

    def get_obligatory_filter_strict(
        self,
        obfilter_id: Optional[str] = None,
        field_guid: Optional[str] = None,
    ) -> ObligatoryFilter:
        return self._ds_accessor.get_obligatory_filter_strict(obfilter_id=obfilter_id, field_guid=field_guid)

    def get_cached_raw_schema(self, role: DataSourceRole) -> list[SchemaColumn]:
        return list(
            chain.from_iterable(
                self.get_data_source_coll_strict(source_id=source_id).get_cached_raw_schema(role=role) or ()
                for source_id in self._ds_accessor.get_data_source_id_list()
            )
        )

    def get_new_raw_schema(
        self,
        role: DataSourceRole,
        conn_executor_factory: Callable[[], SyncConnExecutorBase],
    ) -> list[SchemaColumn]:
        """
        Return raw schema of all data sources
        """
        return list(
            chain.from_iterable(
                (
                    self.get_data_source_coll_strict(source_id=source_id).get_raw_schema(
                        role=role,
                        conn_executor_factory=conn_executor_factory,
                    )
                    or ()
                )
                for source_id in self._ds_accessor.get_data_source_id_list()
            )
        )

    def quote(self, value: str, role: DataSourceRole) -> str:
        sql_dsrc = self.get_sql_data_source_strict(source_id=self._dataset.get_single_data_source_id(), role=role)
        dialect = sql_dsrc.get_dialect()
        return dialect.identifier_preparer.quote(value)

    def resolve_source_role(self, for_preview: bool = False) -> DataSourceRole:
        return self._ds_capabilities.resolve_source_role(for_preview=for_preview)


@attr.s
class EditableDatasetTestWrapper(DatasetTestWrapper):
    _ds_editor: DatasetComponentEditor = attr.ib(init=False)

    @_ds_editor.default
    def _make_ds_editor(self) -> DatasetComponentEditor:
        return DatasetComponentEditor(dataset=self._dataset)

    def update_data_source_collection(
        self,
        source_id: str,
        title: Optional[str] = None,
        valid: Optional[bool] = None,
    ) -> None:
        self._ds_editor.update_data_source_collection(source_id=source_id, title=title, valid=valid)

    def remove_data_source_collection(self, source_id: str) -> None:
        self._ds_editor.remove_data_source_collection(source_id=source_id)

    def add_data_source(
        self,
        *,
        source_id: str,
        role: DataSourceRole = DataSourceRole.origin,
        created_from: DataSourceType,
        connection_id: Optional[str] = None,
        title: Optional[str] = None,
        raw_schema: Optional[list[SchemaColumn]] = None,
        index_info_set: FrozenSet[IndexInfo] = None,
        managed_by: Optional[ManagedBy] = None,
        parameters: Optional[dict[str, Any]] = None,
    ) -> None:
        self._ds_editor.add_data_source(
            source_id=source_id,
            role=role,
            created_from=created_from,
            connection_id=connection_id,
            title=title,
            raw_schema=raw_schema,
            index_info_set=index_info_set,
            managed_by=managed_by,
            parameters=parameters,
        )

    def update_data_source(
        self,
        source_id: str,
        role: Optional[DataSourceRole] = None,
        connection_id: Optional[str] = None,
        created_from: Optional[DataSourceType] = None,
        raw_schema: Optional[list] = None,
        index_info_set: FrozenSet[IndexInfo] = None,
        **parameters: Any,
    ) -> None:
        self._ds_editor.update_data_source(
            source_id=source_id,
            role=role,
            connection_id=connection_id,
            created_from=created_from,
            raw_schema=raw_schema,
            index_info_set=index_info_set,
            **parameters,
        )

    def remove_data_source(self, source_id: str, role: DataSourceRole) -> None:
        self._ds_editor.remove_data_source(source_id=source_id, role=role)

    def add_avatar(
        self,
        avatar_id: str,
        source_id: str,
        title: str,
        managed_by: Optional[ManagedBy] = None,
        valid: bool = True,
    ) -> None:
        return self._ds_editor.add_avatar(
            avatar_id=avatar_id,
            source_id=source_id,
            title=title,
            managed_by=managed_by,
            valid=valid,
        )

    def update_avatar(
        self,
        avatar_id: str,
        source_id: Optional[str] = None,
        title: Optional[str] = None,
        valid: Optional[bool] = None,
    ) -> None:
        return self._ds_editor.update_avatar(
            avatar_id=avatar_id,
            source_id=source_id,
            title=title,
            valid=valid,
        )

    def remove_avatar(self, avatar_id: str) -> None:
        self._ds_editor.remove_avatar(avatar_id=avatar_id)

    def add_avatar_relation(
        self,
        relation_id: str,
        left_avatar_id: str,
        right_avatar_id: str,
        conditions: List[BinaryCondition],
        join_type: JoinType = None,
        managed_by: ManagedBy = None,
        valid: bool = True,
    ) -> None:
        self._ds_editor.add_avatar_relation(
            relation_id=relation_id,
            left_avatar_id=left_avatar_id,
            right_avatar_id=right_avatar_id,
            conditions=conditions,
            join_type=join_type,
            managed_by=managed_by,
            valid=valid,
        )

    def update_avatar_relation(
        self,
        relation_id: str,
        conditions: Optional[List[BinaryCondition]] = None,
        join_type: Optional[JoinType] = None,
        valid: Optional[bool] = None,
    ) -> None:
        self._ds_editor.update_avatar_relation(
            relation_id=relation_id,
            conditions=conditions,
            join_type=join_type,
            valid=valid,
        )

    def remove_avatar_relation(self, relation_id: str) -> None:
        self._ds_editor.remove_avatar_relation(relation_id=relation_id)

    def add_obligatory_filter(
        self,
        obfilter_id: str,
        field_guid: str,
        default_filters: list[DefaultWhereClause],
        managed_by: Optional[ManagedBy] = None,
        valid: bool = True,
    ) -> None:
        self._ds_editor.add_obligatory_filter(
            obfilter_id=obfilter_id,
            field_guid=field_guid,
            default_filters=default_filters,
            managed_by=managed_by,
            valid=valid,
        )

    def update_obligatory_filter(
        self,
        obfilter_id: str,
        default_filters: Optional[list[DefaultWhereClause]] = None,
        valid: Optional[bool] = None,
    ) -> None:
        self._ds_editor.update_obligatory_filter(
            obfilter_id=obfilter_id,
            default_filters=default_filters,
            valid=valid,
        )

    def remove_obligatory_filter(self, obfilter_id: str) -> None:
        self._ds_editor.remove_obligatory_filter(obfilter_id=obfilter_id)

    def set_result_schema(self, result_schema: ResultSchema | Iterable[BIField]) -> None:
        self._ds_editor.set_result_schema(result_schema=result_schema)

    def set_created_via(self, created_via: DataSourceCreatedVia) -> None:
        self._ds_editor.set_created_via(created_via=created_via)
