from collections import defaultdict
from copy import deepcopy
import logging
from typing import (
    Any,
    Iterable,
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
    DefaultConnectionRef,
    DefaultWhereClause,
    ObligatoryFilter,
    connection_ref_from_id,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.connectors.base.data_source_migration import get_data_source_migrator
from dl_core.data_source.type_mapping import get_data_source_class
from dl_core.data_source_merge_tools import (
    make_spec_from_dict,
    update_spec_from_dict,
)
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.db.elements import (
    IndexInfo,
    SchemaColumn,
)
import dl_core.exc as exc
from dl_core.fields import (
    BIField,
    ResultSchema,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    SourceAvatar,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class DatasetComponentEditor:
    _dataset: Dataset = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    def ensure_data_source_exists(self, source_id: str) -> None:
        self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)  # It checks for existence

    def add_data_source_collection(
        self,
        *,
        source_id: str,
        title: str | None,
        managed_by: ManagedBy = ManagedBy.user,
        valid: bool = True,
    ) -> DataSourceCollectionSpec:
        """Add a new data source collection configuration entry. Return its ID"""

        dsrc_coll_spec = DataSourceCollectionSpec(
            id=source_id,
            title=title,
            managed_by=managed_by,
            valid=valid,
        )
        self._dataset.data.source_collections.append(dsrc_coll_spec)
        return dsrc_coll_spec

    def update_data_source_collection(
        self, source_id: str, title: str | None = None, valid: bool | None = None
    ) -> None:
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)

        if title is not None:
            dsrc_coll_spec.title = title

        if valid is not None:
            dsrc_coll_spec.valid = valid

    def remove_data_source_collection(self, source_id: str, ignore_avatars: bool = False) -> None:
        """
        Remove data source collection configuration from the dataset.
        If ``ignore_avatars`` is true, then avatar dependency check is skipped;
        this can be used if the source will be immediately recreated; use with caution.
        """

        if not ignore_avatars:
            bound_avatars = self._ds_accessor.get_avatar_list(source_id=source_id)
            if bound_avatars:
                raise exc.DatasetConfigurationError(
                    "Can't delete source because it is bound by avatars: {}".format([ava.id for ava in bound_avatars])
                )
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        assert dsrc_coll_spec is not None
        self._dataset.data.source_collections.remove(dsrc_coll_spec)

    def add_data_source(
        self,
        *,
        source_id: str,
        role: DataSourceRole = DataSourceRole.origin,
        created_from: DataSourceType,
        connection_id: str | None = None,
        title: str | None = None,
        raw_schema: list[SchemaColumn] | None = None,
        index_info_set: frozenset[IndexInfo] | None = None,
        managed_by: ManagedBy | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        """Add a new data source to the dataset"""

        if not created_from:
            raise ValueError("created_from is required")
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_opt(source_id=source_id)
        if dsrc_coll_spec is None:
            managed_by = managed_by or ManagedBy.user
            assert managed_by is not None
            if role != DataSourceRole.origin:
                raise exc.DatasetConfigurationError(f"No data source collection for ID {source_id}")
            dsrc_coll_spec = self.add_data_source_collection(
                source_id=source_id,
                managed_by=managed_by,
                title=title,
                valid=True,
            )

        assert isinstance(dsrc_coll_spec, DataSourceCollectionSpec)
        dsrc_cls = get_data_source_class(created_from)
        if not dsrc_cls.store_raw_schema:
            raw_schema = None

        connection_ref = connection_ref_from_id(connection_id=connection_id)
        parameters = deepcopy(parameters or {})
        parameters["connection_ref"] = connection_ref
        parameters["raw_schema"] = raw_schema
        parameters["index_info_set"] = index_info_set

        dsrc_spec = make_spec_from_dict(source_type=created_from, data=parameters)

        dsrc_coll_spec.set_for_role(role, dsrc_spec)

    def update_data_source(
        self,
        source_id: str,
        role: DataSourceRole | None = None,
        connection_id: str | None = None,
        created_from: DataSourceType | None = None,
        raw_schema: list | None = None,
        index_info_set: frozenset[IndexInfo] | None = None,
        **parameters: Any,
    ) -> None:
        """Update data source config data"""
        if role is None:
            role = DataSourceRole.origin
        assert role is not None

        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        assert isinstance(dsrc_coll_spec, DataSourceCollectionSpec)
        dsrc_spec = dsrc_coll_spec.get_for_role(role)
        assert dsrc_spec is not None

        if connection_id is not None:
            parameters["connection_ref"] = DefaultConnectionRef(conn_id=connection_id)
        if raw_schema is not None:
            parameters["raw_schema"] = raw_schema
        # If index info was not provided during update we should clear it
        parameters["index_info_set"] = index_info_set

        new_source_type = created_from if created_from is not None else dsrc_spec.source_type
        dsrc_spec = update_spec_from_dict(source_type=new_source_type, data=parameters, old_spec=dsrc_spec)
        dsrc_coll_spec.set_for_role(role=role, value=dsrc_spec)

    def remove_data_source(self, source_id: str, role: DataSourceRole, delete_mat_table: bool = True) -> None:
        """Remove data source configuration from collection"""

        if role == DataSourceRole.origin:
            self.remove_data_source_collection(source_id=source_id)
        else:
            dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)

            if dsrc_coll_spec.managed_by != ManagedBy.user:
                LOGGER.info(f"dsrc_coll_spec.managed_by = {dsrc_coll_spec.managed_by}. Skipping datasource deletion.")
                return

            # We can delete only sources from non-ref collections
            assert isinstance(dsrc_coll_spec, DataSourceCollectionSpec)

            dsrc_spec = dsrc_coll_spec.get_for_role(role=role)
            if dsrc_spec is None:
                return

            # Sample and mat sources must be SQL
            assert isinstance(dsrc_spec, StandardSQLDataSourceSpec)

            dsrc_coll_spec.set_for_role(role, None)

    def _elect_new_root_avatar(self) -> str:
        """Choose among avatars that have no left relations"""
        relations_by_right = defaultdict(list)
        for rel in self._dataset.data.avatar_relations:
            relations_by_right[rel.right_avatar_id].append(rel)
        for avatar in self._dataset.data.source_avatars:
            if not relations_by_right[avatar.id]:
                return avatar.id

        raise RuntimeError("No avatars left to elect from")

    def add_avatar(
        self,
        avatar_id: str,
        source_id: str,
        title: str,
        managed_by: ManagedBy | None = None,
        valid: bool = True,
    ) -> None:
        is_root = not self._dataset.data.source_avatars  # first avatar is always root
        self.ensure_data_source_exists(source_id)
        self._dataset.data.source_avatars.append(
            SourceAvatar(
                id=avatar_id,
                source_id=source_id,
                title=title,
                is_root=is_root,
                managed_by=managed_by or ManagedBy.user,
                valid=valid,
            )
        )

    def update_avatar(
        self,
        avatar_id: str,
        source_id: str | None = None,
        title: str | None = None,
        valid: bool | None = None,
    ) -> None:
        avatar = self._ds_accessor.get_avatar_opt(avatar_id=avatar_id)
        if avatar is not None:
            if source_id is not None:
                self.ensure_data_source_exists(source_id=source_id)
                avatar.source_id = source_id
            if title is not None:
                avatar.title = title
            if valid is not None:
                avatar.valid = valid

    def remove_avatar(self, avatar_id: str) -> None:
        bound_relations = self._ds_accessor.get_avatar_relation_list(
            left_avatar_id=avatar_id
        ) + self._ds_accessor.get_avatar_relation_list(right_avatar_id=avatar_id)
        if bound_relations:
            raise exc.DatasetConfigurationError(
                "Can't delete avatar because it is bound by relations: {}".format([rel.id for rel in bound_relations])
            )

        avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
        self._dataset.data.source_avatars.remove(avatar)

        if avatar.is_root and self._dataset.data.source_avatars:
            self.set_root_avatar(avatar_id=self._elect_new_root_avatar(), rebuild_relations=False)

    def set_root_avatar(self, avatar_id: str, rebuild_relations: bool = True) -> None:
        for avatar in self._dataset.data.source_avatars:
            if avatar.id == avatar_id:
                avatar.is_root = True
            elif avatar.is_root:
                avatar.is_root = False

        if rebuild_relations:
            # root must have no left relations
            relations_to_reverse = []
            current_avatar_id = avatar_id
            while True:
                left_relations = self._ds_accessor.get_avatar_relation_list(right_avatar_id=current_avatar_id)
                if not left_relations:
                    # it is already the leftmost avatar
                    break

                if len(left_relations) != 1:
                    raise exc.DatasetConfigurationError("Can't have more than one left relation")

                relations_to_reverse.append(left_relations[0].id)
                current_avatar_id = left_relations[0].left_avatar_id

            for relation_id in reversed(relations_to_reverse):
                self.reverse_relation(relation_id=relation_id)

    def ensure_avatar_exists(self, avatar_id: str) -> None:
        self._ds_accessor.get_avatar_strict(avatar_id)  # It checks for existence

    def add_avatar_relation(
        self,
        relation_id: str,
        left_avatar_id: str,
        right_avatar_id: str,
        conditions: list[BinaryCondition],
        join_type: JoinType | None = None,
        managed_by: ManagedBy | None = None,
        valid: bool = True,
        required: bool = False,
    ) -> None:
        # validate
        if self._ds_accessor.get_avatar_relation_list(right_avatar_id=right_avatar_id):
            raise exc.DatasetConfigurationError("Avatar {} already has a left relation".format(right_avatar_id))
        for avatar_id in (left_avatar_id, right_avatar_id):
            if avatar_id:  # isn't it always true?
                self.ensure_avatar_exists(avatar_id=avatar_id)

        right_avatar = self._ds_accessor.get_avatar_strict(right_avatar_id)
        if right_avatar.is_root:
            self.set_root_avatar(avatar_id=left_avatar_id, rebuild_relations=False)

        self._dataset.data.avatar_relations.append(
            AvatarRelation(
                id=relation_id,
                left_avatar_id=left_avatar_id,
                right_avatar_id=right_avatar_id,
                conditions=conditions[:],
                join_type=join_type or JoinType.inner,
                managed_by=managed_by or ManagedBy.user,
                valid=valid,
                required=required,
            )
        )

    def update_avatar_relation(
        self,
        relation_id: str,
        conditions: list[BinaryCondition] | None = None,
        join_type: JoinType | None = None,
        valid: bool | None = None,
        required: bool | None = None,
    ) -> None:
        # left/right avatar IDs cannot be changed. Delete/create new relation to do that
        relation = self._ds_accessor.get_avatar_relation_opt(relation_id=relation_id)
        if relation is not None:
            if conditions is not None:
                relation.conditions = conditions[:]
            if join_type is not None:
                relation.join_type = join_type
            if valid is not None:
                relation.valid = valid
            if required is not None:
                relation.required = required

    def reverse_relation(self, relation_id: str) -> None:
        """Swap left and right parts of the relation. This might be needed if root avatar is reset"""
        # TODO
        raise RuntimeError("Relation rebuilding is not supported")

    def remove_avatar_relation(self, relation_id: str) -> None:
        found_ind: int | None = None
        for i, relation_config in enumerate(self._dataset.data.avatar_relations):
            if relation_config.id == relation_id:
                found_ind = i
        if found_ind is not None:
            del self._dataset.data.avatar_relations[found_ind]

    def add_obligatory_filter(
        self,
        obfilter_id: str,
        field_guid: str,
        default_filters: list[DefaultWhereClause],
        managed_by: ManagedBy | None = None,
        valid: bool = True,
    ) -> None:
        for filter_object in self._dataset.data.obligatory_filters:
            if filter_object.field_guid == field_guid:
                raise exc.DatasetConfigurationError(f"Obligatory filter for field {field_guid} already exists.")

        filter_object = ObligatoryFilter(
            id=obfilter_id,
            field_guid=field_guid,
            default_filters=default_filters,
            managed_by=managed_by or ManagedBy.user,
            valid=valid,
        )
        self._dataset.data.obligatory_filters.append(filter_object)

    def update_obligatory_filter(
        self,
        obfilter_id: str,
        default_filters: list[DefaultWhereClause] | None = None,
        valid: bool | None = None,
    ) -> None:
        for filter_object in self._dataset.data.obligatory_filters:
            if filter_object.id == obfilter_id:
                if default_filters is not None:
                    filter_object.default_filters = default_filters
                if valid is not None:
                    filter_object.valid = valid
                return

        raise exc.DatasetConfigurationError(f"Obligatory filter with id {obfilter_id} not found.")

    def remove_obligatory_filter(self, obfilter_id: str) -> None:
        new_filters = []
        for filter_object in self._dataset.data.obligatory_filters:
            if filter_object.id != obfilter_id:
                new_filters.append(filter_object)
        self._dataset.data.obligatory_filters = new_filters

    def set_result_schema(self, result_schema: ResultSchema | Iterable[BIField]) -> None:
        if not isinstance(result_schema, ResultSchema):
            result_schema = ResultSchema(fields=list(result_schema))
        self._dataset.data.result_schema = result_schema

    def set_revision_id(self, revision_id: str | None) -> None:
        self._dataset.data.revision_id = revision_id

    def set_load_preview_by_default(self, load_preview_by_default: bool | None) -> None:
        self._dataset.data.load_preview_by_default = load_preview_by_default

    def set_template_enabled(self, template_enabled: bool | None) -> None:
        self._dataset.data.template_enabled = template_enabled

    def set_data_export_forbidden(self, data_export_forbidden: bool | None) -> None:
        self._dataset.data.data_export_forbidden = data_export_forbidden

    def set_created_via(self, created_via: DataSourceCreatedVia) -> None:
        self._dataset.meta["created_via"] = created_via.name

    def set_description(self, description: str) -> None:
        if self._dataset.annotation is None:
            self._dataset.annotation = {}
        self._dataset.annotation["description"] = description

    def replace_connection(self, old_connection: ConnectionBase, new_connection: ConnectionBase) -> None:
        old_migrator = get_data_source_migrator(old_connection.conn_type)
        new_migrator = get_data_source_migrator(new_connection.conn_type)

        role = DataSourceRole.origin
        for source_id in self._ds_accessor.get_data_source_id_list():
            old_source_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
            assert isinstance(old_source_coll_spec, DataSourceCollectionSpec)
            old_source_spec = old_source_coll_spec.get_for_role(role)
            assert old_source_spec is not None
            migration_dtos = old_migrator.export_migration_dtos(data_source_spec=old_source_spec)
            new_connection_ref = new_connection.conn_ref
            assert new_connection_ref is not None
            new_source_spec = new_migrator.import_migration_dtos(
                migration_dtos=migration_dtos, connection_ref=new_connection_ref
            )
            old_source_coll_spec.set_for_role(role=role, value=new_source_spec)
