from __future__ import annotations

from typing import (
    ClassVar,
    Generator,
    NamedTuple,
    Optional,
    Union,
)

import attr

from dl_constants.enums import (
    ComponentType,
    ManagedBy,
)
from dl_core.base_models import ObligatoryFilter
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.editor import DatasetComponentEditor
from dl_core.data_source import DataSourceCollectionBase
from dl_core.data_source.collection import DataSourceCollectionFactory
import dl_core.exc as common_exc
from dl_core.fields import (
    BIField,
    ResultSchema,
)
from dl_core.multisource import (
    AvatarRelation,
    SourceAvatar,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer


DatasetComponent = Union[
    DataSourceCollectionBase,
    SourceAvatar,
    AvatarRelation,
    BIField,
    ObligatoryFilter,
    ResultSchema,
]


class DatasetComponentRef(NamedTuple):
    component_type: ComponentType
    component_id: str


@attr.s(frozen=True)
class DatasetComponentAbstraction:
    _dataset: Dataset = attr.ib(kw_only=True)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)
    _ds_editor: DatasetComponentEditor = attr.ib(init=False)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(init=False)

    _can_check_valid: ClassVar[frozenset[ComponentType]] = frozenset(
        {
            ComponentType.data_source,
            ComponentType.source_avatar,
            ComponentType.avatar_relation,
            ComponentType.field,
            ComponentType.obligatory_filter,
        }
    )

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    @_ds_editor.default
    def _make_ds_editor(self) -> DatasetComponentEditor:
        return DatasetComponentEditor(dataset=self._dataset)

    @_dsrc_coll_factory.default
    def _make_dsrc_coll_factory(self) -> DataSourceCollectionFactory:
        return DataSourceCollectionFactory(us_entry_buffer=self._us_entry_buffer)

    @property
    def _ds(self) -> Dataset:  # FIXME
        return self._dataset

    def _get_data_source_coll_strict(self, source_id: str) -> DataSourceCollectionBase:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        return dsrc_coll

    def _get_data_source_coll_opt(self, source_id: str) -> Optional[DataSourceCollectionBase]:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_opt(source_id=source_id)
        if not dsrc_coll_spec:
            return None
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)
        return dsrc_coll

    def get_component(self, component_ref: DatasetComponentRef) -> Optional[DatasetComponent]:
        """Get dataset component by its ID and type"""
        if component_ref.component_type == ComponentType.data_source:
            return self._get_data_source_coll_opt(source_id=component_ref.component_id)
        if component_ref.component_type == ComponentType.source_avatar:
            return self._ds_accessor.get_avatar_opt(avatar_id=component_ref.component_id)
        if component_ref.component_type == ComponentType.avatar_relation:
            return self._ds_accessor.get_avatar_relation_opt(relation_id=component_ref.component_id)
        if component_ref.component_type == ComponentType.field:
            for field in self._ds.result_schema:
                if field.guid == component_ref.component_id:
                    return field
            return None
        if component_ref.component_type == ComponentType.obligatory_filter:
            return self._ds_accessor.get_obligatory_filter_opt(obfilter_id=component_ref.component_id)
        if component_ref.component_type == ComponentType.result_schema:
            if self._ds.result_schema.id == component_ref.component_id:
                return self._ds.result_schema
            return None

        raise ValueError(f"Unsupported component_type {component_ref.component_type.name}")

    def update_component_validity(self, component_ref: DatasetComponentRef, valid: bool) -> None:
        """
        Update ``valid`` attribute of given dataset component.
        If component doesn't exist, return peacefully.
        """
        component = self.get_component(component_ref=component_ref)
        if (
            component_ref.component_type not in self._can_check_valid or component is None
        ):  # or valid == component.valid:
            return

        if valid != component.valid:
            if component_ref.component_type == ComponentType.data_source:
                self._ds_editor.update_data_source_collection(source_id=component_ref.component_id, valid=valid)
            elif component_ref.component_type == ComponentType.obligatory_filter:
                self._ds_editor.update_obligatory_filter(obfilter_id=component_ref.component_id, valid=valid)
            elif component_ref.component_type == ComponentType.source_avatar:
                self._ds_editor.update_avatar(avatar_id=component_ref.component_id, valid=valid)
            elif component_ref.component_type == ComponentType.avatar_relation:
                self._ds_editor.update_avatar_relation(relation_id=component_ref.component_id, valid=valid)
            elif component_ref.component_type == ComponentType.field:
                for field_idx, field in enumerate(self._ds.result_schema):
                    if field.guid == component_ref.component_id:
                        self._ds.result_schema.update_field(idx=field_idx, field=field.clone(valid=valid))
            else:
                raise ValueError(f"Unsupported component_type {component_ref.component_type.name}")

    def iter_dataset_components_by_type(self, component_type: ComponentType) -> Generator[DatasetComponent, None, None]:
        """Iterate over dataset components of given type"""
        if component_type == ComponentType.data_source:
            for other_component_id in self._ds_accessor.get_data_source_id_list():
                yield self._get_data_source_coll_strict(source_id=other_component_id)
        elif component_type == ComponentType.source_avatar:
            yield from self._ds_accessor.get_avatar_list()
        elif component_type == ComponentType.avatar_relation:
            yield from self._ds_accessor.get_avatar_relation_list()
        elif component_type == ComponentType.obligatory_filter:
            yield from self._ds_accessor.get_obligatory_filter_list()
        elif component_type == ComponentType.field:
            yield from self._ds.result_schema
        elif component_type == ComponentType.result_schema:
            yield self._ds.result_schema
        else:
            raise ValueError(f"Unsupported component_type {component_type.name}")

    def validate_component_can_be_managed(self, component_ref: DatasetComponentRef, by: Optional[ManagedBy]) -> None:
        if by is not None:
            component = self.get_component(component_ref=component_ref)
            if component.managed_by != by:  # type: ignore  # TODO: fix
                raise common_exc.DatasetConfigurationError(
                    f"Component {component_ref.component_type.name} {component_ref.component_id} "
                    f"cannot be managed by {by.name}"
                )
