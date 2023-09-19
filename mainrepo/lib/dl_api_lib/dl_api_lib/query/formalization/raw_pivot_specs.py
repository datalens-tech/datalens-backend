from __future__ import annotations

from typing import (
    Any,
    Optional,
    TypeVar,
)

import attr

from dl_api_lib.pivot.primitives import PivotMeasureSorting
from dl_constants.enums import (
    OrderDirection,
    PivotRole,
)


_PIVOT_ROLE_SPEC_TV = TypeVar("_PIVOT_ROLE_SPEC_TV", bound="RawPivotRoleSpec")


@attr.s(frozen=True)
class RawPivotRoleSpec:
    role: PivotRole = attr.ib(kw_only=True)

    def clone(self: _PIVOT_ROLE_SPEC_TV, **updates: Any) -> _PIVOT_ROLE_SPEC_TV:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class RawDimensionPivotRoleSpec(RawPivotRoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True, default=OrderDirection.asc)


@attr.s(frozen=True)
class RawAnnotationPivotRoleSpec(RawPivotRoleSpec):
    target_legend_item_ids: Optional[list[int]] = attr.ib(kw_only=True, default=None)
    annotation_type: str = attr.ib(kw_only=True, default="")


@attr.s(frozen=True)
class RawPivotMeasureRoleSpec(RawPivotRoleSpec):
    sorting: Optional[PivotMeasureSorting] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class RawPivotLegendItem:
    pivot_item_id: Optional[int] = attr.ib(kw_only=True, default=None)
    legend_item_ids: list[int] = attr.ib(kw_only=True)
    role_spec: RawPivotRoleSpec = attr.ib(kw_only=True)
    title: Optional[str] = attr.ib(kw_only=True, default=None)

    def clone(self, **updates: Any) -> RawPivotLegendItem:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class RawPivotTotalsItemSpec:
    level: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawPivotTotalsSpec:
    rows: list[RawPivotTotalsItemSpec] = attr.ib(kw_only=True, factory=list)
    columns: list[RawPivotTotalsItemSpec] = attr.ib(kw_only=True, factory=list)

    def clone(self, **updates: Any) -> RawPivotTotalsSpec:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class PivotPaginationSpec:
    offset_rows: Optional[int] = attr.ib(kw_only=True, default=None)
    limit_rows: Optional[int] = attr.ib(kw_only=True, default=None)

    @property
    def non_default(self) -> bool:
        return self.offset_rows is not None or self.limit_rows is not None


@attr.s(frozen=True)
class RawPivotSpec:
    pagination: PivotPaginationSpec = attr.ib(kw_only=True, factory=PivotPaginationSpec)
    structure: list[RawPivotLegendItem] = attr.ib(kw_only=True, factory=list)
    with_totals: Optional[bool] = attr.ib(kw_only=True, default=False)
    totals: Optional[RawPivotTotalsSpec] = attr.ib(kw_only=True, factory=RawPivotTotalsSpec)

    def clone(self, **updates: Any) -> RawPivotSpec:
        return attr.evolve(self, **updates)
