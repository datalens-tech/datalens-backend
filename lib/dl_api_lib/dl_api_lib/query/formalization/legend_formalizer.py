from __future__ import annotations

import abc
import json
from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_api_lib.query.formalization.field_resolver import FieldResolver
from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_specs import (
    MeasureNameRef,
    PlaceholderRef,
    RawFieldSpec,
    RawFilterFieldSpec,
    RawGroupByFieldSpec,
    RawOrderByFieldSpec,
    RawParameterValueSpec,
    RawQuerySpecUnion,
    RawRangeRoleSpec,
    RawRowRoleSpec,
    RawSelectFieldSpec,
    RawTemplateRoleSpec,
    RawTreeRoleSpec,
)
from dl_constants.enums import (
    CalcMode,
    FieldRole,
    FieldType,
    ManagedBy,
    UserDataType,
    WhereClauseOperation,
)
import dl_core.exc as core_exc
from dl_core.fields import BIField
from dl_core.us_dataset import Dataset
import dl_query_processing.exc
from dl_query_processing.legend.field_legend import (
    SELECTABLE_ROLES,
    DimensionNameObjSpec,
    FieldObjSpec,
    FilterRoleSpec,
    Legend,
    LegendItem,
    MeasureNameObjSpec,
    OrderByRoleSpec,
    ParameterRoleSpec,
    PlaceholderObjSpec,
    RangeRoleSpec,
    RoleSpec,
    RowRoleSpec,
    TemplateRoleSpec,
    TreeRoleSpec,
)
from dl_utils.utils import enum_not_none


DATA_TYPES_SUPPORTING_TREE = frozenset(
    {
        UserDataType.tree_str,
    }
)


@attr.s
class LegendFormalizer(abc.ABC):
    SUPPORTS_ROLES: ClassVar[set[FieldRole]]
    SUPPORTS_MEASURE_NAME: ClassVar[bool] = False

    _dataset: Dataset = attr.ib(kw_only=True)
    _autofill_legend: bool = attr.ib(kw_only=True, default=False)
    _field_resolver: FieldResolver = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._field_resolver = FieldResolver(dataset=self._dataset)

    def validate_legend_item(self, item: LegendItem) -> None:
        """Redefine this to impose restrictions on legend items"""
        if item.role_spec.role not in self.SUPPORTS_ROLES:
            raise dl_query_processing.exc.UnsopportedRoleInLegend(
                f"Legend role {item.role_spec.role.name} is not supported"
            )

        if not self.SUPPORTS_MEASURE_NAME and isinstance(item.obj, MeasureNameObjSpec):
            raise dl_query_processing.exc.MeasureNameUnsupported()

        if item.role_spec.role == FieldRole.tree:
            if item.data_type not in DATA_TYPES_SUPPORTING_TREE:
                raise dl_query_processing.exc.RoleDataTypeMismatch("Unsupported data type for tree role")

    def validate_legend(self, legend: Legend) -> None:
        for item in legend:
            self.validate_legend_item(item)

    def _get_field(self, field_id: str) -> BIField:
        return self._dataset.result_schema.by_guid(field_id)

    def _resolve_item_spec(
        self,
        legend_item_id: int,
        item_spec: RawFieldSpec,
        ignore_nonexistent_filters: bool,
    ) -> Optional[LegendItem]:
        # Make role spec
        role_spec: RoleSpec
        if isinstance(item_spec, RawSelectFieldSpec):
            raw_role_spec = item_spec.role_spec
            if raw_role_spec.role == FieldRole.template:
                assert isinstance(raw_role_spec, RawTemplateRoleSpec)
                role_spec = TemplateRoleSpec(
                    role=raw_role_spec.role,
                    template=raw_role_spec.template,
                    visibility=raw_role_spec.visibility,
                )
            elif raw_role_spec.role == FieldRole.tree:
                assert isinstance(raw_role_spec, RawTreeRoleSpec)
                try:
                    prefix = json.loads(raw_role_spec.prefix)
                    if not isinstance(prefix, list):
                        raise ValueError(prefix)
                except ValueError as e:
                    raise dl_query_processing.exc.GenericInvalidRequestError("Invalid value for tree prefix") from e
                role_spec = TreeRoleSpec(
                    role=raw_role_spec.role,
                    level=raw_role_spec.level,
                    prefix=prefix,
                    dimension_values=raw_role_spec.dimension_values,
                    visibility=raw_role_spec.visibility,
                )
            elif raw_role_spec.role == FieldRole.range:
                assert isinstance(raw_role_spec, RawRangeRoleSpec)
                role_spec = RangeRoleSpec(role=raw_role_spec.role, range_type=raw_role_spec.range_type)
            elif raw_role_spec.role == FieldRole.row:
                assert isinstance(raw_role_spec, RawRowRoleSpec)
                role_spec = RowRoleSpec(role=raw_role_spec.role, visibility=raw_role_spec.visibility)
            else:
                role_spec = RoleSpec(role=raw_role_spec.role)
        elif isinstance(item_spec, RawGroupByFieldSpec):
            role_spec = RowRoleSpec(role=FieldRole.row)
        elif isinstance(item_spec, RawOrderByFieldSpec):
            role_spec = OrderByRoleSpec(role=FieldRole.order_by, direction=item_spec.direction)
        elif isinstance(item_spec, RawFilterFieldSpec):
            role_spec = FilterRoleSpec(role=FieldRole.filter, operation=item_spec.operation, values=item_spec.values)
        elif isinstance(item_spec, RawParameterValueSpec):
            role_spec = ParameterRoleSpec(role=FieldRole.parameter, value=item_spec.value)
        else:
            raise TypeError(str(type(item_spec)))

        # Make the legend item
        legend_item: LegendItem
        if isinstance(item_spec.ref, MeasureNameRef):
            legend_item = LegendItem(
                legend_item_id=legend_item_id,
                obj=MeasureNameObjSpec(),
                block_id=item_spec.block_id,
                role_spec=role_spec,
                data_type=UserDataType.string,
                field_type=FieldType.DIMENSION,
            )
        elif isinstance(item_spec.ref, PlaceholderRef):
            legend_item = LegendItem(
                legend_item_id=legend_item_id,
                obj=PlaceholderObjSpec(),
                block_id=item_spec.block_id,
                role_spec=role_spec,
                data_type=UserDataType.string,
                field_type=FieldType.DIMENSION,
            )
        else:
            try:
                field_id = self._field_resolver.field_id_from_spec(item_spec.ref)
            except core_exc.FieldNotFound:
                if role_spec.role == FieldRole.filter and ignore_nonexistent_filters:
                    return None
                raise
            field = self._get_field(field_id)
            legend_item = LegendItem(
                legend_item_id=legend_item_id,
                block_id=item_spec.block_id,
                obj=FieldObjSpec(id=field_id, title=field.title),
                role_spec=role_spec,
                data_type=enum_not_none(field.data_type),
                field_type=field.type,
            )

        return legend_item

    def _validate_explicit_legend_item_ids(self, raw_query_spec_union: RawQuerySpecUnion) -> set[int]:
        explicit_ids = [
            spec.legend_item_id for spec in raw_query_spec_union.iter_item_specs() if spec.legend_item_id is not None
        ]
        unique_explicit_ids = set(explicit_ids)
        if len(explicit_ids) != len(unique_explicit_ids):
            raise dl_query_processing.exc.NonUniqueLegendIdsError("Got non-unique legend item IDs")
        return unique_explicit_ids

    def _generate_info_items(self, legend: Legend, id_gen: IdGenerator) -> None:
        """
        Fill legend with all fields from result_schema that are not already there,
        use role info so that they are in no way used in the data request or pivot.
        """
        found_field_ids: set[str] = {item.id for item in legend}
        for field in self._dataset.result_schema:
            if field.guid in found_field_ids:
                continue
            legend.add_item(
                LegendItem(
                    legend_item_id=id_gen.generate_id(),
                    obj=FieldObjSpec(id=field.guid, title=field.title),
                    data_type=field.data_type,
                    field_type=field.type,
                    role_spec=RoleSpec(role=FieldRole.info),
                )
            )

    def _generate_tree_filter_items(self, legend: Legend, id_gen: IdGenerator) -> None:
        """
        Generate additional filters for each item with role tree.
        Use the same block_id as in the original item.
        """

        for item in legend.list_for_role(role=FieldRole.tree):
            tree_spec = item.role_spec
            assert isinstance(tree_spec, TreeRoleSpec)

            # Generate filters for the tree field itself
            filter_role_specs: list[FilterRoleSpec] = []
            filter_role_specs.append(
                FilterRoleSpec(
                    role=FieldRole.filter,
                    operation=WhereClauseOperation.LENGTE,
                    values=[tree_spec.level],
                )
            )
            if len(tree_spec.prefix) > 0:
                filter_role_specs.append(
                    FilterRoleSpec(
                        role=FieldRole.filter,
                        operation=WhereClauseOperation.STARTSWITH,
                        values=[tree_spec.prefix],
                    )
                )
            for filter_role_spec in filter_role_specs:
                filter_item = LegendItem(
                    obj=item.obj,
                    data_type=item.data_type,
                    field_type=item.field_type,
                    role_spec=filter_role_spec,
                    legend_item_id=id_gen.generate_id(),
                    block_id=item.block_id,
                )
                legend.add_item(filter_item)

            # Generate filters for locked dimensions
            for dim_spec in tree_spec.dimension_values:
                locked_dim_item = legend.get_item(dim_spec.legend_item_id)
                if locked_dim_item.id == item.id:
                    # Ignore filter for the tree itself
                    continue
                filter_role_spec = FilterRoleSpec(
                    role=FieldRole.filter,
                    operation=WhereClauseOperation.EQ,
                    values=[dim_spec.value],
                )
                filter_item = LegendItem(
                    obj=locked_dim_item.obj,
                    data_type=locked_dim_item.data_type,
                    field_type=locked_dim_item.field_type,
                    role_spec=filter_role_spec,
                    legend_item_id=id_gen.generate_id(),
                    block_id=item.block_id,
                )
                legend.add_item(filter_item)

    def patch_legend(self, legend: Legend, id_gen: IdGenerator) -> None:
        """Patch the legend if something is missing"""

        self._generate_tree_filter_items(legend=legend, id_gen=id_gen)

        if self._autofill_legend:
            self._generate_info_items(legend=legend, id_gen=id_gen)

    def generate_legend_items(self, raw_query_spec_union: RawQuerySpecUnion, id_gen: IdGenerator) -> list[LegendItem]:
        items: list[LegendItem] = []
        already_used_field_ids: set[str] = set()
        for item_spec in raw_query_spec_union.iter_item_specs():
            if isinstance(item_spec, RawGroupByFieldSpec):
                # Ignore group_by fields unless this field is not present in select
                if self._field_resolver.field_id_from_spec(item_spec.ref) in already_used_field_ids:
                    continue

            item = self._resolve_item_spec(
                legend_item_id=(
                    item_spec.legend_item_id if item_spec.legend_item_id is not None else id_gen.generate_id()
                ),
                item_spec=item_spec,
                ignore_nonexistent_filters=raw_query_spec_union.ignore_nonexistent_filters,
            )
            if item is None:
                # Skip it
                continue

            self.validate_legend_item(item)
            items.append(item)
            if item.role_spec.role in SELECTABLE_ROLES:
                already_used_field_ids.add(item.id)

        return items

    def make_legend(self, raw_query_spec_union: RawQuerySpecUnion) -> Legend:
        used_ids = self._validate_explicit_legend_item_ids(raw_query_spec_union)
        id_gen = IdGenerator(used_ids=used_ids)
        items = self.generate_legend_items(raw_query_spec_union=raw_query_spec_union, id_gen=id_gen)
        legend = Legend(items=items)
        self.patch_legend(legend=legend, id_gen=id_gen)
        self.validate_legend(legend)
        return legend


@attr.s
class ResultLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.order_by,
        FieldRole.filter,
        FieldRole.parameter,
        FieldRole.info,
        FieldRole.row,
        FieldRole.total,
        FieldRole.template,
        FieldRole.tree,
        FieldRole.measure,
    }


@attr.s
class PreviewLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.order_by,
        FieldRole.filter,
        FieldRole.parameter,
        FieldRole.info,
        FieldRole.row,
        FieldRole.measure,
    }

    def generate_legend_items(self, raw_query_spec_union: RawQuerySpecUnion, id_gen: IdGenerator) -> list[LegendItem]:
        assert not raw_query_spec_union.select_specs  # preview's select is always autogenerated # FIXME: DL exception
        items: list[LegendItem] = []

        for field in self._dataset.result_schema:
            if field.hidden:
                continue
            if field.managed_by != ManagedBy.user:
                continue
            if field.calc_mode == CalcMode.parameter:
                continue

            item = LegendItem(
                legend_item_id=id_gen.generate_id(),
                obj=FieldObjSpec(id=field.guid, title=field.title),
                data_type=field.data_type,
                field_type=field.type,
                role_spec=RowRoleSpec(role=FieldRole.row),
            )
            items.append(item)

        return items


@attr.s
class DistinctLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.order_by,
        FieldRole.filter,
        FieldRole.parameter,
        FieldRole.info,
        FieldRole.distinct,
    }

    def validate_legend_item(self, item: LegendItem) -> None:
        super().validate_legend_item(item)
        if item.role_spec.role == FieldRole.filter and item.field_type == FieldType.MEASURE:
            raise dl_query_processing.exc.MeasureFilterUnsupportedError()

    def validate_legend(self, legend: Legend) -> None:
        super().validate_legend(legend=legend)
        if len(legend.list_for_role(FieldRole.distinct)) != 1:
            raise dl_query_processing.exc.LegendError("Legend must have one item")


@attr.s
class RangeLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.filter,
        FieldRole.parameter,
        FieldRole.info,
        FieldRole.range,
    }

    def validate_legend_item(self, item: LegendItem) -> None:
        super().validate_legend_item(item)
        if item.role_spec.role == FieldRole.filter and item.field_type == FieldType.MEASURE:
            raise dl_query_processing.exc.MeasureFilterUnsupportedError()

    def validate_legend(self, legend: Legend) -> None:
        super().validate_legend(legend=legend)
        if len(legend.list_for_role(FieldRole.range)) > 2:
            raise dl_query_processing.exc.LegendError("Legend must have one or two range items")


@attr.s
class PivotLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.order_by,
        FieldRole.filter,
        FieldRole.parameter,
        FieldRole.info,
        FieldRole.row,
        FieldRole.measure,
        FieldRole.template,
        FieldRole.total,
    }
    SUPPORTS_MEASURE_NAME = True

    def patch_legend(self, legend: Legend, id_gen: IdGenerator) -> None:
        super().patch_legend(legend=legend, id_gen=id_gen)

        # Always add `Measure Name` to the legend for pivot tables if it is not already there
        mname_legend_item_ids = legend.get_measure_name_legend_item_ids()
        if not mname_legend_item_ids:
            legend.add_item(
                LegendItem(
                    legend_item_id=id_gen.generate_id(),
                    obj=MeasureNameObjSpec(),
                    # using `row` here would corrupt the expected structure, so use `info`
                    role_spec=RoleSpec(role=FieldRole.info),
                    data_type=UserDataType.string,
                    field_type=FieldType.DIMENSION,
                )
            )

        # Same goes for `Dimension Name`
        dname_legend_item_ids = legend.get_dimension_name_legend_item_ids()
        if not dname_legend_item_ids:
            legend.add_item(
                LegendItem(
                    legend_item_id=id_gen.generate_id(),
                    obj=DimensionNameObjSpec(),
                    # using `row` here would corrupt the expected structure, so use `info`
                    role_spec=RoleSpec(role=FieldRole.info),
                    data_type=UserDataType.string,
                    field_type=FieldType.DIMENSION,
                )
            )


@attr.s
class ValidationLegendFormalizer(LegendFormalizer):
    SUPPORTS_ROLES = {
        FieldRole.row,
    }

    def generate_legend_items(self, raw_query_spec_union: RawQuerySpecUnion, id_gen: IdGenerator) -> list[LegendItem]:
        """Assume that all fields participate in the query"""

        items: list[LegendItem] = []

        for field in self._dataset.result_schema:
            if field.managed_by != ManagedBy.user:
                continue

            item = LegendItem(
                legend_item_id=id_gen.generate_id(),
                obj=FieldObjSpec(id=field.guid, title=field.title),
                data_type=field.data_type,
                field_type=field.type,
                role_spec=RowRoleSpec(role=FieldRole.row),
            )
            items.append(item)

        return items
