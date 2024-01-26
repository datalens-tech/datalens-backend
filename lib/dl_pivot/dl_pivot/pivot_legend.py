from __future__ import annotations

from collections import defaultdict
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
)

import attr

from dl_constants.enums import (
    OrderDirection,
    PivotItemType,
    PivotRole,
)
from dl_pivot.primitives import PivotMeasureSorting
import dl_query_processing.exc


@attr.s(frozen=True)
class PivotRoleSpec:
    role: PivotRole = attr.ib(kw_only=True)


@attr.s(frozen=True)
class PivotDimensionRoleSpec(PivotRoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True, default=OrderDirection.asc)


@attr.s(frozen=True)
class PivotAnnotationRoleSpec(PivotRoleSpec):
    annotation_type: str = attr.ib(kw_only=True)
    target_legend_item_ids: Optional[list[int]] = attr.ib(
        kw_only=True, default=None
    )  # if None, then applies to all measures


@attr.s(frozen=True)
class PivotMeasureRoleSpec(PivotRoleSpec):
    sorting: Optional[PivotMeasureSorting] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class PivotLegendItem:
    pivot_item_id: int = attr.ib(kw_only=True)
    legend_item_ids: list[int] = attr.ib(kw_only=True)
    role_spec: PivotRoleSpec = attr.ib(kw_only=True)
    title: str = attr.ib(kw_only=True)
    item_type: PivotItemType = attr.ib(kw_only=True, default=PivotItemType.stream_item)


@attr.s
class PivotLegend:
    _items: List[PivotLegendItem] = attr.ib(kw_only=True)
    _piid_to_item: Dict[int, PivotLegendItem] = attr.ib(init=False)
    _liid_to_piid: Dict[int, list[int]] = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._piid_to_item = {}
        self._liid_to_piid = defaultdict(list)
        self._repopulate_mappings()

    def _repopulate_mappings(self) -> None:
        self._piid_to_item.clear()
        self._liid_to_piid.clear()
        for item in self._items:
            self._piid_to_item[item.pivot_item_id] = item
            for legend_item_id in item.legend_item_ids:
                self._liid_to_piid[legend_item_id].append(item.pivot_item_id)

    @property
    def items(self) -> Sequence[PivotLegendItem]:
        return self._items

    def get_item(self, pivot_item_id: int) -> PivotLegendItem:
        try:
            return self._piid_to_item[pivot_item_id]
        except KeyError:
            raise dl_query_processing.exc.PivotLegendItemReferenceError(f"Unknown pivot legend item: {pivot_item_id}")

    def leg_item_id_to_pivot_item_id_list(self, legend_item_id: int) -> list[int]:
        return self._liid_to_piid[legend_item_id]

    def add_item(self, item: PivotLegendItem) -> None:
        self._items.append(item)
        self._repopulate_mappings()

    def list_for_role(self, role: PivotRole) -> Sequence[PivotLegendItem]:
        return tuple(item for item in self._items if item.role_spec.role == role)

    def get_measure_name_pivot_item_ids(self) -> List[int]:
        return [item.pivot_item_id for item in self.items if item.item_type == PivotItemType.measure_name]

    def get_dimension_name_pivot_item_ids(self) -> List[int]:
        return [item.pivot_item_id for item in self.items if item.item_type == PivotItemType.dimension_name]

    def get_unused_pivot_item_id(self) -> int:
        return max(item.pivot_item_id for item in self._items) + 1
