from __future__ import annotations

from itertools import chain
from typing import AbstractSet, Any, ClassVar, Iterable, List, Optional, TypeVar, Union

import attr

from bi_constants.internal_constants import MEASURE_NAME_ID, DIMENSION_NAME_ID, PLACEHOLDER_ID
from bi_constants.enums import (
    FieldRole, FieldVisibility, OrderDirection, WhereClauseOperation, QueryBlockPlacementType, RangeType,
)

from bi_query_processing.enums import QueryType, GroupByPolicy
from bi_query_processing.base_specs.dimensions import DimensionValueSpec
from bi_query_processing.postprocessing.primitives import PostprocessedValue


FilterArgType = Union[str, int, float, None]


@attr.s(frozen=True)
class FieldRef:
    pass


@attr.s(frozen=True)
class IdFieldRef(FieldRef):
    id: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class TitleFieldRef(FieldRef):
    title: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class IdOrTitleFieldRef(FieldRef):
    id_or_title: str = attr.ib(kw_only=True)  # ID or title, depending on the context


@attr.s(frozen=True)
class MeasureNameRef(FieldRef):
    id: str = attr.ib(kw_only=True, init=False, default=MEASURE_NAME_ID)


@attr.s(frozen=True)
class DimensionNameRef(FieldRef):
    id: str = attr.ib(kw_only=True, init=False, default=DIMENSION_NAME_ID)


@attr.s(frozen=True)
class PlaceholderRef(FieldRef):
    id: str = attr.ib(kw_only=True, init=False, default=PLACEHOLDER_ID)


_FIELD_SPEC_TV = TypeVar('_FIELD_SPEC_TV', bound='RawFieldSpec')


@attr.s(frozen=True)
class RawFieldSpec:
    ref: FieldRef = attr.ib(kw_only=True, default=None)
    legend_item_id: Optional[int] = attr.ib(kw_only=True, default=None)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)

    def clone(self: _FIELD_SPEC_TV, **updates: Any) -> _FIELD_SPEC_TV:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class RawRoleSpec:
    role: FieldRole = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawOrderByRoleSpec(RawRoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawRangeRoleSpec(RawRoleSpec):
    range_type: RangeType = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawDimensionRoleSpec(RawRoleSpec):
    visibility: FieldVisibility = attr.ib(kw_only=True, default=FieldVisibility.visible)


@attr.s(frozen=True)
class RawRowRoleSpec(RawDimensionRoleSpec):
    pass


@attr.s(frozen=True)
class RawTemplateRoleSpec(RawDimensionRoleSpec):
    template: str = attr.ib(kw_only=True, default='')


@attr.s(frozen=True)
class RawTreeRoleSpec(RawDimensionRoleSpec):
    level: int = attr.ib(kw_only=True)
    prefix: str = attr.ib(kw_only=True)
    dimension_values: List[DimensionValueSpec] = attr.ib(kw_only=True, factory=list)


@attr.s(frozen=True)
class RawSelectFieldSpec(RawFieldSpec):
    role_spec: RawRoleSpec = attr.ib(kw_only=True, default=RawRowRoleSpec(role=FieldRole.row))
    label: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class RawGroupByFieldSpec(RawFieldSpec):
    pass


@attr.s(frozen=True)
class RawOrderByFieldSpec(RawFieldSpec):  # noqa
    """
    Ambiguous ORDER BY field clause specifier that serves as input from the API,
    where either field ID or title can be specified.
    """
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawFilterFieldSpec(RawFieldSpec):  # noqa
    """
    Ambiguous filter specifier
    """
    operation: WhereClauseOperation = attr.ib(kw_only=True)
    values: List[FilterArgType] = attr.ib(kw_only=True)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class RawParameterValueSpec(RawFieldSpec):
    value: Any = attr.ib(kw_only=True, default=None)


@attr.s
class RawQueryMetaInfo:
    query_type: QueryType = attr.ib(kw_only=True, default=QueryType.result)
    row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)

    def clone(self, **updates: Any) -> RawQueryMetaInfo:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class RawQuerySpecUnion:
    # TODO: Combine with RawQuerySpecUnion
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    group_by_policy: GroupByPolicy = attr.ib(kw_only=True, default=GroupByPolicy.force)
    disable_rls: bool = attr.ib(kw_only=True, default=False)
    ignore_nonexistent_filters: bool = attr.ib(kw_only=True, default=True)
    allow_measure_fields: bool = attr.ib(kw_only=True, default=True)
    meta: RawQueryMetaInfo = attr.ib(kw_only=True, factory=RawQueryMetaInfo)
    # item lists
    select_specs: List[RawSelectFieldSpec] = attr.ib(kw_only=True, factory=list)
    group_by_specs: List[RawGroupByFieldSpec] = attr.ib(kw_only=True, factory=list)
    order_by_specs: List[RawOrderByFieldSpec] = attr.ib(kw_only=True, factory=list)
    filter_specs: List[RawFilterFieldSpec] = attr.ib(kw_only=True, factory=list)
    parameter_value_specs: List[RawParameterValueSpec] = attr.ib(kw_only=True, factory=list)
    block_specs: List[RawBlockSpec] = attr.ib(kw_only=True, factory=list)

    def clone(self, **updates: Any) -> RawQuerySpecUnion:
        return attr.evolve(self, **updates)

    def iter_item_specs(self) -> Iterable[RawFieldSpec]:
        return chain(
            self.select_specs,
            self.group_by_specs,
            self.order_by_specs,
            self.filter_specs,
            self.parameter_value_specs,
        )

    def get_unique_block_ids(self) -> AbstractSet[int]:
        return {
            spec.block_id for spec in self.iter_item_specs()
            if spec.block_id is not None
        }


@attr.s(frozen=True)
class RawDimensionValueSpec:
    legend_item_id: int = attr.ib(kw_only=True)
    value: PostprocessedValue = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawBlockPlacement:
    type: ClassVar[QueryBlockPlacementType]


@attr.s(frozen=True)
class RawRootBlockPlacement(RawBlockPlacement):
    type = QueryBlockPlacementType.root


@attr.s(frozen=True)
class RawAfterBlockPlacement(RawBlockPlacement):
    type = QueryBlockPlacementType.after

    dimension_values: Optional[List[RawDimensionValueSpec]] = attr.ib(default=None)


@attr.s
class RawBlockSpec:
    block_id: int = attr.ib(kw_only=True)
    parent_block_id: Optional[int] = attr.ib(kw_only=True)
    placement: RawBlockPlacement = attr.ib(kw_only=True, factory=RawRootBlockPlacement)
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)


def spec_is_field_name_pseudo_dimension(spec: RawFieldSpec) -> bool:
    return isinstance(spec.ref, (MeasureNameRef, DimensionNameRef))


@attr.s(frozen=True)
class RawResultSpec:
    with_totals: bool = attr.ib(kw_only=True, default=False)
