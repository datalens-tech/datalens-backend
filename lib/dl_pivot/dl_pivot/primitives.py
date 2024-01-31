from __future__ import annotations

from enum import (
    Enum,
    auto,
    unique,
)
from typing import (
    Hashable,
    NamedTuple,
    Optional,
)

import attr

from dl_constants.enums import (
    OrderDirection,
    PivotHeaderRole,
)


class DataCell(NamedTuple):
    value: Hashable
    legend_item_id: int
    pivot_item_id: int


class DataCellVector(NamedTuple):
    cells: tuple[DataCell, ...]

    @property
    def main_cell(self) -> DataCell:
        return self.cells[0]

    @property
    def main_value(self) -> Hashable:
        return self[0][0][0]  # self.cells[0].value

    @property
    def main_legend_item_id(self) -> int:
        return self[0][0][1]  # self.cells[0].legend_item_id

    @property
    def main_pivot_item_id(self) -> int:
        return self[0][0][2]  # self.cells[0].pivot_item_id


class MeasureNameValue(NamedTuple):
    value: Hashable  # The actual measure name
    measure_piid: int  # legend item ID of the referenced measure () for handling of duplicate measures in table


@attr.s
class PivotHeaderRoleSpec:
    role: PivotHeaderRole = attr.ib(kw_only=True, default=PivotHeaderRole.data)


@attr.s(frozen=True)
class PivotHeaderValue:
    value: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class PivotMeasureSortingSettings:
    header_values: list[PivotHeaderValue] = attr.ib(kw_only=True)
    direction: OrderDirection = attr.ib(kw_only=True, default=OrderDirection.asc)
    role_spec: PivotHeaderRoleSpec = attr.ib(kw_only=True, factory=PivotHeaderRoleSpec)


@attr.s(frozen=True)
class PivotMeasureSorting:
    column: Optional[PivotMeasureSortingSettings] = attr.ib(kw_only=True, default=None)
    row: Optional[PivotMeasureSortingSettings] = attr.ib(kw_only=True, default=None)


@attr.s(slots=True)
class PivotHeaderInfo:
    sorting_direction: Optional[OrderDirection] = attr.ib(kw_only=True, default=None)
    role_spec: PivotHeaderRoleSpec = attr.ib(kw_only=True, factory=PivotHeaderRoleSpec)


@attr.s(slots=True)
class PivotHeader:
    values: tuple[DataCellVector, ...] = attr.ib(kw_only=True, default=())
    info: PivotHeaderInfo = attr.ib(kw_only=True, factory=PivotHeaderInfo)

    def main_header_values(self) -> list[Hashable]:
        return [value.main_value for value in self.values]

    @staticmethod
    def _normalize_value(value: Hashable) -> Hashable:
        if isinstance(value, MeasureNameValue):
            return value.value
        return value

    def compare_sorting_settings(self, sorting_settings: PivotMeasureSortingSettings) -> bool:
        if self.info.role_spec != sorting_settings.role_spec:
            return False

        main_values = self.main_header_values()
        sorting_values: list[Hashable] = [header_value.value for header_value in sorting_settings.header_values]
        if not main_values:
            return not sorting_values

        last_value = main_values.pop()
        main_values = list(map(self._normalize_value, main_values))
        if isinstance(last_value, MeasureNameValue):
            # the last Measure name value may be hidden, so try to compare without it first
            if sorting_values == main_values:
                return True

            main_values.append(last_value.value)
            return sorting_values == main_values
        main_values.append(last_value)
        return sorting_values == main_values


MeasureValues = tuple[Optional[DataCellVector], ...]


class DataRow(NamedTuple):
    header: PivotHeader
    values: MeasureValues


@unique
class SortAxis(Enum):
    columns = auto()
    rows = auto()
