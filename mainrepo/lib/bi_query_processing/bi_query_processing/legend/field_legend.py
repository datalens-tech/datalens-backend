from __future__ import annotations

from collections import defaultdict
from typing import (
    Any,
    Collection,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
)

import attr

from bi_constants.enums import (
    BIType,
    FieldRole,
    FieldType,
    FieldVisibility,
    LegendItemType,
    OrderDirection,
    RangeType,
    WhereClauseOperation,
)
from bi_constants.internal_constants import (
    DIMENSION_NAME_ID,
    DIMENSION_NAME_TITLE,
    MEASURE_NAME_ID,
    MEASURE_NAME_TITLE,
    PLACEHOLDER_ID,
    PLACEHOLDER_TITLE,
)
from bi_query_processing.base_specs.dimensions import DimensionValueSpec
import bi_query_processing.exc


@attr.s(frozen=True)
class RoleSpec:
    role: FieldRole = attr.ib(kw_only=True, default=FieldRole.row)


@attr.s(frozen=True)
class OrderByRoleSpec(RoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s(frozen=True)
class FilterRoleSpec(RoleSpec):
    operation: WhereClauseOperation = attr.ib(kw_only=True)
    values: List[Any] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ParameterRoleSpec(RoleSpec):
    value: Any = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RangeRoleSpec(RoleSpec):
    range_type: RangeType = attr.ib(kw_only=True)


@attr.s(frozen=True)
class DimensionRoleSpec(RoleSpec):
    visibility: FieldVisibility = attr.ib(kw_only=True, default=FieldVisibility.visible)


@attr.s(frozen=True)
class RowRoleSpec(DimensionRoleSpec):
    pass


@attr.s(frozen=True)
class TemplateRoleSpec(DimensionRoleSpec):
    template: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class TreeRoleSpec(DimensionRoleSpec):
    level: int = attr.ib(kw_only=True)
    prefix: list = attr.ib(kw_only=True)
    dimension_values: List[DimensionValueSpec] = attr.ib(kw_only=True, factory=list)


@attr.s(frozen=True)
class ObjSpec:
    item_type: LegendItemType = attr.ib(kw_only=True)


@attr.s(frozen=True)
class MeasureNameObjSpec(ObjSpec):
    item_type: LegendItemType = attr.ib(kw_only=True, default=LegendItemType.measure_name)


@attr.s(frozen=True)
class DimensionNameObjSpec(ObjSpec):
    item_type: LegendItemType = attr.ib(kw_only=True, default=LegendItemType.dimension_name)


@attr.s(frozen=True)
class PlaceholderObjSpec(ObjSpec):
    item_type: LegendItemType = attr.ib(kw_only=True, default=LegendItemType.placeholder)


@attr.s(frozen=True)
class FieldObjSpec(ObjSpec):
    item_type: LegendItemType = attr.ib(kw_only=True, default=LegendItemType.field)
    id: str = attr.ib(kw_only=True)  # Field ID
    title: str = attr.ib(kw_only=True)  # Field title


@attr.s(frozen=True)
class LegendItem:
    legend_item_id: int = attr.ib(kw_only=True)
    obj: ObjSpec = attr.ib(kw_only=True)
    role_spec: RoleSpec = attr.ib(kw_only=True, factory=RoleSpec)
    data_type: BIType = attr.ib(kw_only=True)
    field_type: FieldType = attr.ib(kw_only=True)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)

    @property
    def item_type(self) -> LegendItemType:
        return self.obj.item_type

    @property
    def id(self) -> str:
        # TODO: Remove
        if self.obj.item_type == LegendItemType.measure_name:
            return MEASURE_NAME_ID
        if self.obj.item_type == LegendItemType.dimension_name:
            return DIMENSION_NAME_ID
        if self.obj.item_type == LegendItemType.placeholder:
            return PLACEHOLDER_ID
        assert isinstance(self.obj, FieldObjSpec)
        return self.obj.id

    @property
    def title(self) -> str:
        # TODO: Remove
        if self.obj.item_type == LegendItemType.measure_name:
            return MEASURE_NAME_TITLE
        if self.obj.item_type == LegendItemType.dimension_name:
            return DIMENSION_NAME_TITLE
        if self.obj.item_type == LegendItemType.placeholder:
            return PLACEHOLDER_TITLE
        assert isinstance(self.obj, FieldObjSpec)
        return self.obj.title

    def clone(self, **updates: Any) -> LegendItem:
        return attr.evolve(self, **updates)


NON_SELECTABLE_ROLES = {
    FieldRole.info,
    FieldRole.order_by,
    FieldRole.filter,
    FieldRole.parameter,
}
SELECTABLE_ROLES = set(FieldRole) - NON_SELECTABLE_ROLES


@attr.s
class Legend:
    _items: List[LegendItem] = attr.ib(kw_only=True)
    _liid_to_item: Dict[int, LegendItem] = attr.ib(init=False)
    _liid_to_field_id_map: Dict[int, str] = attr.ib(init=False)
    _field_id_to_liid_list_map: Dict[str, List[int]] = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._liid_to_item = {}
        self._liid_to_field_id_map = {}
        self._field_id_to_liid_list_map = defaultdict(list)
        self._repopulate_mappings()

    @property
    def items(self) -> Sequence[LegendItem]:
        return self._items

    def get_item(self, legend_item_id: int) -> LegendItem:
        try:
            return self._liid_to_item[legend_item_id]
        except KeyError:
            raise bi_query_processing.exc.LegendItemReferenceError(f"Unknown legend item: {legend_item_id}")

    def add_item(self, item: LegendItem) -> None:
        self._items.append(item)
        self._repopulate_mappings()

    def _repopulate_mappings(self) -> None:
        self._liid_to_item.clear()
        self._liid_to_field_id_map.clear()
        self._field_id_to_liid_list_map.clear()
        for item in self._items:
            self._liid_to_item[item.legend_item_id] = item
            self._liid_to_field_id_map[item.legend_item_id] = item.id
            self._field_id_to_liid_list_map[item.id].append(item.legend_item_id)

    def get_measure_name_legend_item_ids(self) -> List[int]:
        return self.field_id_to_leg_item_id_list(MEASURE_NAME_ID)

    def get_dimension_name_legend_item_ids(self) -> List[int]:
        return self.field_id_to_leg_item_id_list(DIMENSION_NAME_ID)

    def field_id_to_leg_item_id_list(self, field_id: str) -> List[int]:
        return self._field_id_to_liid_list_map[field_id]

    def __iter__(self) -> Iterator[LegendItem]:
        return iter(self._items)

    def list_for_role(self, role: FieldRole) -> Sequence[LegendItem]:
        return tuple(item for item in self._items if item.role_spec.role == role)

    def limit_to_roles(self, roles: Collection[FieldRole]) -> Legend:
        return Legend(items=[item for item in self.items if item.role_spec.role in roles])

    def limit_to_block(self, block_id: int) -> Legend:
        return Legend(
            items=[item for item in self.items if item.block_id is None or item.block_id == block_id],
        )

    def list_selectable_items(self) -> List[LegendItem]:
        """List the items that can be selected from the data source"""
        items = [
            item
            for item in self.items
            if item.role_spec.role in SELECTABLE_ROLES and isinstance(item.obj, FieldObjSpec)
        ]
        return items

    def list_streamable_items(self) -> Sequence[LegendItem]:
        """List streamable items + special derived ones like templates"""
        items = [
            item
            for item in self.items
            if item.role_spec.role in SELECTABLE_ROLES and isinstance(item.obj, (FieldObjSpec, PlaceholderObjSpec))
        ]
        return items
