from __future__ import annotations

import logging
import os
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Collection,
    Optional,
    Sequence,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_api_lib.query.formalization.avatar_tools import normalize_explicit_avatar_ids
from dl_api_lib.query.formalization.field_resolver import FieldResolver
from dl_api_lib.query.formalization.query_formalizer_base import QuerySpecFormalizerBase
from dl_constants.enums import (
    CalcMode,
    DataSourceRole,
    FieldRole,
    FieldType,
    OrderDirection,
    RangeType,
    UserDataType,
    WhereClauseOperation,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.dependencies.factory_base import ComponentDependencyManagerFactoryBase
from dl_core.components.ids import (
    AvatarId,
    FieldId,
)
from dl_core.constants import DataAPILimits
from dl_core.data_source.base import DataSource
from dl_core.data_source.collection import DataSourceCollectionFactory
import dl_core.exc
from dl_core.fields import BIField
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_query_processing.compilation.specs import (
    ArrayPrefixSelectWrapperSpec,
    FilterFieldSpec,
    FilterSourceColumnSpec,
    GroupByFieldSpec,
    OrderByFieldSpec,
    ParameterValueSpec,
    RelationSpec,
    SelectFieldSpec,
    SelectWrapperSpec,
)
from dl_query_processing.enums import (
    GroupByPolicy,
    QueryType,
    SelectValueType,
)
import dl_query_processing.exc
from dl_query_processing.legend.field_legend import (
    FilterRoleSpec,
    OrderByRoleSpec,
    ParameterRoleSpec,
    RangeRoleSpec,
    RoleSpec,
    TreeRoleSpec,
)


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry
    from dl_core.us_entry import USEntry
    from dl_query_processing.compilation.query_meta import QueryMetaInfo
    from dl_query_processing.legend.block_legend import BlockSpec


LOGGER = logging.getLogger(__name__)


_RANGE_TYPE_TO_SELECT_TYPE = {
    RangeType.min: SelectWrapperSpec(type=SelectValueType.min),
    RangeType.max: SelectWrapperSpec(type=SelectValueType.max),
}


def need_permission_on_entry(us_entry: USEntry, permission: str) -> None:
    # TODO: DELETE ME after the check is moved up the stack
    assert us_entry.permissions is not None
    assert us_entry.uuid is not None
    if not us_entry.permissions[permission]:
        raise dl_core.exc.USPermissionRequired(us_entry.uuid, permission)


@attr.s
class SimpleQuerySpecFormalizer(QuerySpecFormalizerBase):  # noqa
    """
    Simple formalizer.
    Can be used for validation (no real data query is performed).
    """

    _dataset: Dataset = attr.ib(kw_only=True)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)
    _dsrc_coll_factory: DataSourceCollectionFactory = attr.ib(init=False)
    _field_resolver: FieldResolver = attr.ib(init=False)
    _avatar_resolution_cache: dict[str, bool] = attr.ib(init=False, factory=dict)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    @_dsrc_coll_factory.default
    def _make_dsrc_coll_factory(self) -> DataSourceCollectionFactory:
        return DataSourceCollectionFactory(us_entry_buffer=self._us_entry_buffer)

    @_field_resolver.default
    def _make_field_resolver(self) -> FieldResolver:
        return FieldResolver(dataset=self._dataset)

    def _get_data_source_strict(self, source_id: str, role: DataSourceRole) -> DataSource:
        assert role is not None
        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=source_id)
        dataset_parameter_values = self._ds_accessor.get_parameter_values()
        dsrc_coll = self._dsrc_coll_factory.get_data_source_collection(
            spec=dsrc_coll_spec,
            dataset_parameter_values=dataset_parameter_values,
        )
        dsrc = dsrc_coll.get_strict(role=role)
        return dsrc

    def _is_preview(self, block_spec: BlockSpec) -> bool:
        return block_spec.query_type == QueryType.preview

    def make_phantom_select_ids(
        self,
        block_spec: BlockSpec,
        order_by_specs: Sequence[OrderByFieldSpec],
    ) -> list[FieldId]:
        return []

    def _avatar_exists(self, avatar_id: str) -> bool:
        try:
            return self._avatar_resolution_cache[avatar_id]
        except KeyError:
            avatar_exists = self._ds_accessor.get_avatar_opt(avatar_id=avatar_id) is not None
            self._avatar_resolution_cache[avatar_id] = avatar_exists
            return avatar_exists

    def validate_select_field(self, block_spec: BlockSpec, field: BIField) -> None:
        pass

    def make_select_specs(
        self,
        block_spec: BlockSpec,
        phantom_select_ids: list[FieldId],
    ) -> list[SelectFieldSpec]:
        select_specs: list[SelectFieldSpec] = []
        select_spec_set: set[SelectFieldSpec] = set()
        for legend_select_spec in block_spec.legend.list_selectable_items():
            select_role_spec = legend_select_spec.role_spec

            select_wrapper: SelectWrapperSpec
            if select_role_spec.role == FieldRole.range:
                assert isinstance(select_role_spec, RangeRoleSpec)
                select_wrapper = _RANGE_TYPE_TO_SELECT_TYPE[select_role_spec.range_type]
            elif select_role_spec.role == FieldRole.tree:
                assert isinstance(select_role_spec, TreeRoleSpec)
                select_wrapper = self._make_tree_wrapper(select_role_spec)
            else:
                select_wrapper = SelectWrapperSpec(type=SelectValueType.plain)

            field_id = legend_select_spec.id
            select_spec = SelectFieldSpec(field_id=field_id, wrapper=select_wrapper)
            if select_spec not in select_spec_set:
                select_specs.append(select_spec)
                select_spec_set.add(select_spec)

        for field_id in phantom_select_ids:
            select_spec = SelectFieldSpec(
                field_id=field_id,
                wrapper=SelectWrapperSpec(type=SelectValueType.plain),
            )
            if select_spec not in select_specs:
                select_specs.append(select_spec)
                select_spec_set.add(select_spec)

        for select_spec in select_specs:
            field = self._dataset.result_schema.by_guid(select_spec.field_id)
            self.validate_select_field(block_spec=block_spec, field=field)

        return select_specs

    def make_group_by_specs(
        self,
        block_spec: BlockSpec,
        select_specs: Sequence[SelectFieldSpec],
    ) -> list[GroupByFieldSpec]:
        group_by_specs: list[GroupByFieldSpec] = []
        group_by_spec_set: set[GroupByFieldSpec] = set()
        for select_spec in select_specs:
            field = self._dataset.result_schema.by_guid(select_spec.field_id)
            if field.type == FieldType.DIMENSION:
                group_by_spec = GroupByFieldSpec(
                    field_id=select_spec.field_id,
                    wrapper=select_spec.wrapper,
                )
                if group_by_spec not in group_by_spec_set:
                    group_by_specs.append(group_by_spec)
                    group_by_spec_set.add(group_by_spec)

        return group_by_specs

    def make_filter_specs(
        self,
        block_spec: BlockSpec,
    ) -> list[FilterFieldSpec]:
        result: list[FilterFieldSpec] = []
        for legend_filter_spec in block_spec.legend.list_for_role(FieldRole.filter):
            filter_role_spec = legend_filter_spec.role_spec
            assert isinstance(filter_role_spec, FilterRoleSpec)
            try:
                field_id = legend_filter_spec.id
            except dl_core.exc.FieldNotFound:
                if block_spec.ignore_nonexistent_filters:
                    self._log_info("Skipping filter for unknown field %s", legend_filter_spec)
                    continue
                else:
                    raise

            filter_spec = FilterFieldSpec(
                field_id=field_id,
                operation=filter_role_spec.operation,
                values=filter_role_spec.values,
            )
            result.append(filter_spec)

        return result

    def _make_tree_wrapper(self, role_spec: RoleSpec) -> ArrayPrefixSelectWrapperSpec:
        assert isinstance(role_spec, TreeRoleSpec)
        return ArrayPrefixSelectWrapperSpec(length=role_spec.level)

    def make_order_by_specs(
        self,
        block_spec: BlockSpec,
    ) -> list[OrderByFieldSpec]:
        selectable_items = block_spec.legend.list_selectable_items()
        result: list[OrderByFieldSpec] = []
        for legend_order_by_spec in block_spec.legend.list_for_role(FieldRole.order_by):
            order_by_role_spec = legend_order_by_spec.role_spec
            assert isinstance(order_by_role_spec, OrderByRoleSpec)
            field_id = legend_order_by_spec.id
            selectable_matches = [item for item in selectable_items if item.id == field_id]
            select_wrapper: SelectWrapperSpec = SelectWrapperSpec(type=SelectValueType.plain)
            if selectable_matches:
                first_selectable_match = selectable_matches[0]
                if first_selectable_match.role_spec.role == FieldRole.tree:
                    # Special case: if we're selecting this field (and wrapping it) as a tree,
                    # then it must also be wrapped for ORDER BY
                    select_wrapper = self._make_tree_wrapper(first_selectable_match.role_spec)
            order_by_spec = OrderByFieldSpec(
                field_id=field_id,
                direction=order_by_role_spec.direction,
                wrapper=select_wrapper,
            )
            result.append(order_by_spec)

        return result

    def make_source_column_filter_specs(self) -> list[FilterSourceColumnSpec]:
        return []

    def make_relation_and_avatar_specs(
        self,
        used_field_ids: Collection[FieldId],
    ) -> tuple[list[RelationSpec], AbstractSet[AvatarId], Optional[AvatarId]]:
        return [], set(), None

    def make_parameter_value_specs(
        self,
        block_spec: BlockSpec,
    ) -> list[ParameterValueSpec]:
        result: list[ParameterValueSpec] = []
        for legend_parameter_spec in block_spec.legend.list_for_role(FieldRole.parameter):
            parameter_role_spec = legend_parameter_spec.role_spec
            assert isinstance(parameter_role_spec, ParameterRoleSpec)
            try:
                field_id = legend_parameter_spec.id
            except dl_core.exc.FieldNotFound:
                if block_spec.ignore_nonexistent_filters:
                    self._log_info("Skipping parameter value for unknown field %s", legend_parameter_spec)
                    continue
                else:
                    raise

            parameter_value_spec = ParameterValueSpec(field_id=field_id, value=parameter_role_spec.value)
            result.append(parameter_value_spec)

        return result


@attr.s
class DataQuerySpecFormalizer(SimpleQuerySpecFormalizer):  # noqa
    """
    A more complex formalizer for real data queries:
    ``/result``, ``/preview``, etc.

    Since this is used for real data queries, more ad-hoc filters,
    additional select ID generators and validations kick in here.

    Ordering by fields omitted from SELECT.
        If some IDs are contained in ORDER BY, but not in SELECT,
        then add them as ``phantom_select_ids`` to enable this kind of ordering.

    RLS.
        Most data queries must be executed with RLS filters
        if such filters are defined. So additional filter specs are generated here.

    Source column filters.
        Some data sources may define filters that apply to their raw columns.
        If such filters exist, then they are enforced.

    Preview & hidden fields.
        Some of the dataset's fields may be marked as "hidden".
        Among other things this means that such fields should be ignored
        in preview queries. So exclude them from the SELECT list.

    Cache-based features & uncached fields.
        Selected fields (or, in general, fields used in the query somehow)
        may include function calls that require the usage of cache-based features
        such as Geocoding. Problems can arise if caches for these features
        have not been generated yet. The handling of fields
        that are affected by missing cache data is controlled
        by the query type - if it is preview,
        such fields are ignored, otherwise errors are raised if such fields are used.

    ``GROUP BY`` control
        In some cases we don't want to do ``GROUP BY`` at all - it saves a couple
        of glaciers from melting and preserves the data in its original form.
        The only problem is that it's is not possible to have aggregations in a
        query ``GROUP BY``-less query without collapsing all rows into a single
        one, so some validation is done against that scenario.

    """

    _rci: RequestContextInfo = attr.ib(kw_only=True)
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dep_mgr_factory: ComponentDependencyManagerFactoryBase = attr.ib(kw_only=True)
    _service_registry: ServicesRegistry = attr.ib(kw_only=True)

    def _validate_phantom_select_ids(self, phantom_select_ids: Sequence[FieldId]) -> None:
        if any(
            self._dataset.result_schema.by_guid(phantom_select_id).type == FieldType.DIMENSION
            for phantom_select_id in phantom_select_ids
        ):
            # FIXME: Not really an InvalidFieldError... replace with some other exception
            raise dl_core.exc.InvalidFieldError("Order by dimension must be in select")

    def _ensure_not_measure(self, field: BIField) -> None:
        if field.type != FieldType.DIMENSION:
            raise dl_query_processing.exc.LogicError(
                f"Request does not support measure fields. Measure field: {field.guid}"
            )

    def _ensure_not_unsupported_type(self, field: BIField) -> None:
        if field.cast == UserDataType.unsupported:
            raise dl_query_processing.exc.LogicError(f"Cannot select fields of unsupported type: {field.title}")

    def validate_select_field(self, block_spec: BlockSpec, field: BIField) -> None:
        super().validate_select_field(block_spec=block_spec, field=field)

        field_id = field.guid

        if field.calc_mode == CalcMode.direct:
            avatar_id = field.avatar_id
            assert avatar_id is not None
            if not self._avatar_exists(avatar_id=avatar_id):
                raise dl_core.exc.UnknownReferencedAvatar(
                    f"Field {field.title!r} ({field_id}) references unknown source avatar {field.avatar_id}."
                )

        if not field.valid:
            # FIXME: BI-2714 Investigate if/why this error is happening and return the raise
            # raise exc.InvalidFieldError(
            LOGGER.error(f"Field {field.title!r} ({field_id}) is invalid and cannot be selected. Error ignored.")

        self._ensure_not_unsupported_type(field)
        if not block_spec.allow_measure_fields:
            self._ensure_not_measure(field)

    def _make_rls_filter_specs(self) -> list[FilterFieldSpec]:
        user_id = self._rci.user_id
        if not user_id:
            raise RuntimeError("No subject to use in RLS")

        result: list[FilterFieldSpec] = []
        restrictions = self._dataset.rls.get_user_restrictions(user_id=user_id)
        for field_guid, values in restrictions.items():
            result.append(
                FilterFieldSpec(
                    field_id=field_guid,
                    operation=WhereClauseOperation.IN,
                    values=values,  # type: ignore  # TODO: fix
                    anonymous=True,
                )
            )
        self._log_info("RLS filters for user %s: %s", user_id, result)
        return result

    def make_phantom_select_ids(
        self,
        block_spec: BlockSpec,
        order_by_specs: Sequence[OrderByFieldSpec],
    ) -> list[FieldId]:
        order_by_id_set: set[str] = {order_by_spec.field_id for order_by_spec in order_by_specs}
        select_id_set = {legend_select_spec.id for legend_select_spec in block_spec.legend.list_selectable_items()}
        phantom_select_ids = list(order_by_id_set - select_id_set)
        self._validate_phantom_select_ids(phantom_select_ids)
        return phantom_select_ids

    def make_filter_specs(self, block_spec: BlockSpec) -> list[FilterFieldSpec]:
        filter_specs = super().make_filter_specs(block_spec=block_spec)

        if block_spec.disable_rls:
            need_permission_on_entry(self._dataset, "admin")  # TODO: move it up the stack
            LOGGER.info(
                "Switching off RLS for request: " 'got "disable_rls" parameter and user has admin role for dataset'
            )

        if not block_spec.disable_rls:
            filter_specs += self._make_rls_filter_specs()

        return filter_specs

    def make_source_column_filter_specs(self) -> list[FilterSourceColumnSpec]:
        source_column_filter_specs = super().make_source_column_filter_specs()

        # This is not quite right, because we collect filters from all avatars,
        # even unused in current request. So we can break joins optimizations.
        # It's better to collect source filters after all other request parts have been processed -
        # only for really useful avatars.
        avatar_ids = [avatar.id for avatar in self._ds_accessor.get_avatar_list()]

        for avatar_id in sorted(avatar_ids):
            avatar = self._ds_accessor.get_avatar_strict(avatar_id=avatar_id)
            dsrc = self._get_data_source_strict(source_id=avatar.source_id, role=self._role)
            dsrc_filters = dsrc.get_filters(service_registry=self._service_registry)
            if dsrc_filters:
                self._log_info("Filters for source %s: %s", dsrc.id, dsrc_filters)

            for dsrc_filter_spec in dsrc_filters:
                source_column_filter_spec = FilterSourceColumnSpec(
                    avatar_id=avatar_id,
                    column_name=dsrc_filter_spec.name,
                    operation=dsrc_filter_spec.operation,
                    values=dsrc_filter_spec.values,
                )
                source_column_filter_specs.append(source_column_filter_spec)

        return source_column_filter_specs

    def make_group_by_specs(
        self,
        block_spec: BlockSpec,
        select_specs: Sequence[SelectFieldSpec],
    ) -> list[GroupByFieldSpec]:
        has_measures = any(
            self._dataset.result_schema.by_guid(spec.field_id).type == FieldType.MEASURE for spec in select_specs
        )

        if block_spec.group_by_policy == GroupByPolicy.if_measures:
            # If there are no measures, then don't GROUP BY
            if not has_measures:
                return []
            # Otherwise fall back to default grouping
        elif block_spec.group_by_policy == GroupByPolicy.disable:
            # GROUP BY is disabled. If there are measures in the query, it is not valid
            if has_measures:
                raise dl_query_processing.exc.InvalidGroupByConfiguration(
                    "Invalid parameter disable_group_by for dataset with measure fields"
                )
            return []

        # Default grouping
        return super().make_group_by_specs(block_spec=block_spec, select_specs=select_specs)

    def make_relation_and_avatar_specs(
        self,
        used_field_ids: Collection[FieldId],
    ) -> tuple[list[RelationSpec], AbstractSet[AvatarId], Optional[AvatarId]]:
        # Resolve avatars explicitly specified in fields
        deep_dep_mgr = self._dep_mgr_factory.get_field_deep_inter_dependency_manager()
        field_ava_dep_mgr = self._dep_mgr_factory.get_field_avatar_dependency_manager()
        tree_resolver = self._dep_mgr_factory.get_avatar_tree_resolver()
        required_field_ids = set(used_field_ids) | {
            ref_field_id
            for field_id in used_field_ids
            for ref_field_id in deep_dep_mgr.get_field_deep_references(field_id)
        }
        explicitly_required_avatar_ids = {
            avatar_id
            for field_id in required_field_ids
            for avatar_id in field_ava_dep_mgr.get_field_avatar_references(field_id)
        }

        # Add avatars that participate in required relations
        avatar_ids_by_required_relations: set[AvatarId] = {
            avatar_id
            for relation in self._ds_accessor.get_avatar_relation_list()
            for avatar_id in (relation.left_avatar_id, relation.right_avatar_id)
            if relation.required
        }
        LOGGER.info(f"Adding avatars that are a part of required relations: {avatar_ids_by_required_relations}")
        explicitly_required_avatar_ids |= avatar_ids_by_required_relations

        # Normalize avatars (fix them if there are no user-managed ones)
        explicitly_required_avatar_ids = normalize_explicit_avatar_ids(
            dataset=self._dataset, required_avatar_ids=explicitly_required_avatar_ids
        )
        if not explicitly_required_avatar_ids:
            return [], set(), self._ds_accessor.get_root_avatar_strict().id

        # Resolve the "missing links" and get the required relation IDs
        root_avatar_id, required_avatar_ids, required_relation_ids = tree_resolver.expand_required_avatar_ids(
            required_avatar_ids=explicitly_required_avatar_ids
        )
        relation_specs = [RelationSpec(relation_id=relation_id) for relation_id in sorted(required_relation_ids)]
        return relation_specs, required_avatar_ids, root_avatar_id

    def make_limit_offset(self, block_spec: BlockSpec) -> tuple[Optional[int], Optional[int]]:
        # Make defaults
        limit, offset = super().make_limit_offset(block_spec=block_spec)

        if self._is_preview(block_spec=block_spec) and self._role != DataSourceRole.sample:
            # Using direct/materialization mode, so we must to limit the number of entries
            limit = min(block_spec.limit or DataAPILimits.PREVIEW_ROW_LIMIT, DataAPILimits.PREVIEW_ROW_LIMIT)

        return limit, offset

    def make_query_meta(
        self,
        block_spec: BlockSpec,
        phantom_select_ids: list[FieldId],
        select_specs: list[SelectFieldSpec],
        root_avatar_id: Optional[AvatarId],
    ) -> QueryMetaInfo:
        query_meta = super().make_query_meta(
            block_spec=block_spec,
            phantom_select_ids=phantom_select_ids,
            select_specs=select_specs,
            root_avatar_id=root_avatar_id,
        )

        allow_subquery = os.environ.get("ALLOW_SUBQUERY_IN_PREVIEW", "0") != "0"  # FIXME: Do not get from env
        # No point in using subquery when fetching from sample - the table is already pretty small there,
        # so disable it by default
        if self._is_preview(block_spec=block_spec) and self._role != DataSourceRole.sample:
            # using direct/materialization mode, so we must to limit the number of entries
            from_subquery = bool(allow_subquery)

            if root_avatar_id is not None:
                avatar = self._ds_accessor.get_avatar_strict(avatar_id=root_avatar_id)
                dsrc = self._get_data_source_strict(source_id=avatar.source_id, role=self._role)
                from_subquery = from_subquery and dsrc.supports_preview_from_subquery

            if from_subquery != query_meta.from_subquery:
                query_meta = query_meta.clone(from_subquery=from_subquery)

        return query_meta


class NoGroupByQuerySpecFormalizerBase(DataQuerySpecFormalizer):
    def make_group_by_specs(
        self,
        block_spec: BlockSpec,
        select_specs: Sequence[SelectFieldSpec],
    ) -> list[GroupByFieldSpec]:
        return []


class ValueDistinctSpecFormalizer(NoGroupByQuerySpecFormalizerBase):
    def make_select_specs(
        self,
        block_spec: BlockSpec,
        phantom_select_ids: list[FieldId],
    ) -> list[SelectFieldSpec]:
        select_specs = super().make_select_specs(block_spec=block_spec, phantom_select_ids=phantom_select_ids)
        assert len(select_specs) == 1
        return select_specs

    def make_order_by_specs(
        self,
        block_spec: BlockSpec,
    ) -> list[OrderByFieldSpec]:
        legend_select_spec_list = block_spec.legend.list_selectable_items()
        assert len(legend_select_spec_list) == 1
        legend_select_spec = legend_select_spec_list[0]
        return [
            OrderByFieldSpec(
                field_id=legend_select_spec.id,
                direction=OrderDirection.asc,
                wrapper=SelectWrapperSpec(type=SelectValueType.plain),
            )
        ]


class SingleRowQuerySpecFormalizerBase(NoGroupByQuerySpecFormalizerBase):
    def make_order_by_specs(
        self,
        block_spec: BlockSpec,
    ) -> list[OrderByFieldSpec]:
        return []

    def make_limit_offset(self, block_spec: BlockSpec) -> tuple[Optional[int], Optional[int]]:
        return (None, None)


class ValueRangeSpecFormalizer(SingleRowQuerySpecFormalizerBase):
    def validate_select_field(self, block_spec: BlockSpec, field: BIField) -> None:
        if field.type != FieldType.DIMENSION:
            raise dl_query_processing.exc.LogicError("Value range can only be fetched for dimensions")

    def make_select_specs(
        self,
        block_spec: BlockSpec,
        phantom_select_ids: list[FieldId],
    ) -> list[SelectFieldSpec]:
        select_specs = super().make_select_specs(block_spec=block_spec, phantom_select_ids=phantom_select_ids)
        assert len(select_specs) == 2
        assert all(
            select_spec.wrapper.type in (SelectValueType.min, SelectValueType.max) for select_spec in select_specs
        )
        return select_specs


class TotalsSpecFormalizer(DataQuerySpecFormalizer):
    pass
