from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    List,
    Optional,
)

import attr

from bi_constants.enums import QueryBlockPlacementType
from bi_query_processing.base_specs.dimensions import (
    DimensionSpec,
    DimensionValueSpec,
)
from bi_query_processing.enums import (
    EmptyQueryMode,
    GroupByPolicy,
    QueryType,
)
from bi_query_processing.legend.field_legend import Legend


@attr.s(frozen=True)
class BlockPlacement:
    type: ClassVar[QueryBlockPlacementType]


@attr.s(frozen=True)
class RootBlockPlacement(BlockPlacement):
    type = QueryBlockPlacementType.root


@attr.s(frozen=True)
class AfterBlockPlacement(BlockPlacement):
    type = QueryBlockPlacementType.after

    dimension_values: Optional[List[DimensionValueSpec]] = attr.ib(default=None)


@attr.s(frozen=True)
class DispersedAfterBlockPlacement(BlockPlacement):
    type = QueryBlockPlacementType.dispersed_after

    parent_dimensions: List[DimensionSpec] = attr.ib(default=None)
    child_dimensions: List[DimensionSpec] = attr.ib(default=None)


@attr.s(frozen=True)
class BlockSpec:
    block_id: int = attr.ib(kw_only=True)
    parent_block_id: Optional[int] = attr.ib(kw_only=True)  # FIXME: Maybe remove this altogether
    placement: BlockPlacement = attr.ib(kw_only=True, factory=RootBlockPlacement)
    legend_item_ids: List[int] = attr.ib(kw_only=True, default=None)
    legend: Legend = attr.ib(kw_only=True)
    group_by_policy: GroupByPolicy = attr.ib(kw_only=True, default=GroupByPolicy.force)
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    query_type: QueryType = attr.ib(kw_only=True, default=QueryType.result)
    row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    disable_rls: bool = attr.ib(kw_only=True, default=False)
    ignore_nonexistent_filters: bool = attr.ib(kw_only=True, default=True)
    allow_measure_fields: bool = attr.ib(kw_only=True, default=True)
    empty_query_mode: EmptyQueryMode = attr.ib(kw_only=True, default=EmptyQueryMode.error)

    def clone(self, **updates: Any) -> BlockSpec:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class BlockLegendMeta:
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)

    def clone(self, **updates: Any) -> BlockLegendMeta:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class BlockLegend:
    blocks: List[BlockSpec] = attr.ib(kw_only=True)
    meta: BlockLegendMeta = attr.ib(kw_only=True, factory=BlockLegendMeta)

    def clone(self, **updates: Any) -> BlockLegend:
        return attr.evolve(self, **updates)
