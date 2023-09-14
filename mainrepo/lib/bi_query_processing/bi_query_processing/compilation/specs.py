from __future__ import annotations

from typing import (
    AbstractSet,
    Any,
    Optional,
    TypeVar,
    Union,
)

import attr

from bi_constants.enums import (
    OrderDirection,
    WhereClauseOperation,
)
from bi_core.components.ids import (
    AvatarId,
    FieldId,
    RelationId,
)
from bi_query_processing.compilation.query_meta import QueryMetaInfo
from bi_query_processing.enums import SelectValueType

FilterArgType = Union[str, int, float, None]

_QUERY_ITEM_SPEC_TV = TypeVar("_QUERY_ITEM_SPEC_TV", bound="QueryItemSpec")


@attr.s(frozen=True)
class QueryItemSpec:
    def clone(self: _QUERY_ITEM_SPEC_TV, **kwargs: Any) -> _QUERY_ITEM_SPEC_TV:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class QueryItemFieldSpec(QueryItemSpec):
    """
    Base class for field-oriented specs
    """

    field_id: FieldId = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SelectWrapperSpec:
    """
    SELECT clause item wrapper specifier.
    """

    type: SelectValueType = attr.ib(kw_only=True, default=SelectValueType.plain)


@attr.s(frozen=True)
class ArrayPrefixSelectWrapperSpec(SelectWrapperSpec):
    """
    Array prefix selection wrapper (for trees).
    """

    type: SelectValueType = attr.ib(kw_only=True, default=SelectValueType.array_prefix)
    length: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SelectFieldSpec(QueryItemFieldSpec):
    """
    SELECT clause item specifier.
    """

    wrapper: SelectWrapperSpec = attr.ib(kw_only=True, factory=SelectWrapperSpec)


@attr.s(frozen=True)
class GroupByFieldSpec(QueryItemFieldSpec):
    """
    GROUP BY clause item specifier.
    """

    wrapper: SelectWrapperSpec = attr.ib(kw_only=True, factory=SelectWrapperSpec)


@attr.s(frozen=True)
class OrderByFieldSpec(QueryItemFieldSpec):
    """
    ORDER BY field clause specifier.
    """

    wrapper: SelectWrapperSpec = attr.ib(kw_only=True, factory=SelectWrapperSpec)
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s(frozen=True)
class FilterFieldSpec(QueryItemFieldSpec):
    """
    Field-based filter specifier (for WHERE and HAVING)
    """

    operation: WhereClauseOperation = attr.ib(kw_only=True)
    values: list[FilterArgType] = attr.ib(kw_only=True)
    # Indicates that the filter formula should not have an `original_field_id`.
    # For usage in RLS filters so that BFB clauses cannot be used to avoid RLS restrictions
    anonymous: bool = attr.ib(kw_only=True, default=False)


@attr.s(frozen=True)
class FilterSourceColumnSpec(QueryItemSpec):
    """
    Table column-based filter specifier (for WHERE and HAVING)
    """

    avatar_id: FieldId = attr.ib(kw_only=True)
    column_name: str = attr.ib(kw_only=True)
    operation: WhereClauseOperation = attr.ib(kw_only=True)
    values: list[FilterArgType] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RelationSpec(QueryItemSpec):
    """
    Relation (ON clause) specifier
    """

    relation_id: RelationId = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ParameterValueSpec(QueryItemFieldSpec):
    """
    Parameter value specifier
    """

    value: Any = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class QuerySpec:
    select_specs: list[SelectFieldSpec] = attr.ib(kw_only=True)
    group_by_specs: list[GroupByFieldSpec] = attr.ib(kw_only=True)
    order_by_specs: list[OrderByFieldSpec] = attr.ib(kw_only=True)
    filter_specs: list[FilterFieldSpec] = attr.ib(kw_only=True)
    relation_specs: list[RelationSpec] = attr.ib(kw_only=True)
    source_column_filter_specs: list[FilterSourceColumnSpec] = attr.ib(kw_only=True)
    parameter_value_specs: list[ParameterValueSpec] = attr.ib(kw_only=True)
    limit: Optional[int] = attr.ib(kw_only=True)
    offset: Optional[int] = attr.ib(kw_only=True)
    root_avatar_id: Optional[AvatarId] = attr.ib(kw_only=True, default=None)
    required_avatar_ids: AbstractSet[AvatarId] = attr.ib(kw_only=True, factory=frozenset)
    meta: QueryMetaInfo = attr.ib(kw_only=True, factory=QueryMetaInfo)
