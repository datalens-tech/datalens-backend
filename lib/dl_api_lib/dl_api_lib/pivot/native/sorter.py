from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Sequence,
    cast,
)

import attr

from dl_api_lib.pivot.base.sorter import PivotSorter
from dl_api_lib.pivot.native.data_frame import (
    FlatPivotDataKey,
    NativePivotDataFrame,
)
from dl_api_lib.pivot.primitives import (
    DataCellVector,
    SortAxis,
)
from dl_api_lib.pivot.sort_helpers import invert
from dl_api_lib.query.formalization.pivot_legend import (
    PivotDimensionRoleSpec,
    PivotMeasureRoleSpec,
)
from dl_constants.enums import (
    OrderDirection,
    PivotRole,
)


if TYPE_CHECKING:
    from dl_api_lib.pivot.primitives import PivotMeasureSortingSettings


@attr.s
class NativePivotSorter(PivotSorter):
    @property
    def _native_dframe(self) -> NativePivotDataFrame:
        assert isinstance(self._pivot_dframe, NativePivotDataFrame)
        return self._pivot_dframe

    def _resolve_axis_order(self) -> dict[SortAxis, list[OrderDirection]]:
        return {
            SortAxis.columns: [
                cast(PivotDimensionRoleSpec, f.role_spec).direction
                for f in self._pivot_legend.list_for_role(role=PivotRole.pivot_column)
            ],
            SortAxis.rows: [
                cast(PivotDimensionRoleSpec, f.role_spec).direction
                for f in self._pivot_legend.list_for_role(role=PivotRole.pivot_row)
            ],
        }

    def _get_key_list_for_axis(self, axis: SortAxis) -> list[FlatPivotDataKey]:
        match axis:
            case SortAxis.rows:
                return self._native_dframe.row_keys
            case SortAxis.columns:
                return self._native_dframe.column_keys
            case _:
                raise ValueError(axis)

    def _sort_key_list(self, key_list: list[FlatPivotDataKey], directions: Sequence[OrderDirection]) -> None:
        all_descending = all(direct is OrderDirection.desc for direct in directions)
        # `all_descending` is an optimization to avoid applying the inversion wrapper to every single value
        # when everything is being sorted in descending order.
        # In this case we just reverse the global `sort` call.

        def normalize_single_dim_value(value: DataCellVector, dim_idx: int) -> Any:
            pivot_item_id = value.main_pivot_item_id
            direction = directions[dim_idx]
            normalizer = self._dimension_sort_strategy.get_normalizer(
                pivot_item_id=pivot_item_id,
                direction=direction,
            )
            value = normalizer.normalize_vector_value(value)
            if not all_descending and direction is OrderDirection.desc:
                # No inversion is applied if all dimensions are in descending order (`all_descending`)
                # because a global `reverse` is applied (see above and below)
                value = invert(value)
            return value

        def normalize_key(key: FlatPivotDataKey) -> Any:
            """Convert key to a value that can be sorted natively"""
            return tuple(
                normalize_single_dim_value(value=dim_val, dim_idx=dim_idx) for dim_idx, dim_val in enumerate(key.values)
            )

        key_list.sort(key=normalize_key, reverse=all_descending)

    def _single_axis_sort(self, axis: SortAxis, directions: Sequence[OrderDirection]) -> None:
        key_list = self._get_key_list_for_axis(axis)
        self._sort_key_list(key_list=key_list, directions=directions)

    def _sort_by_measure(self, axis: SortAxis, sorting_piid: int, settings: PivotMeasureSortingSettings) -> None:
        pass  # TODO: Not implemented yet. Feeling bored? Go right ahead and contribute!

    def sort(self) -> None:
        # First sort by dimension
        directions_by_axis = self._resolve_axis_order()
        for axis in (SortAxis.rows, SortAxis.columns):
            if not directions_by_axis[axis]:
                # Nothing to sort here
                continue
            self._single_axis_sort(axis=axis, directions=directions_by_axis[axis])

        # Now sort by measure (possibly overriding sorting by dimensions)
        for pivot_item in self._pivot_legend.list_for_role(role=PivotRole.pivot_measure):
            sorting_settings = cast(PivotMeasureRoleSpec, pivot_item.role_spec).sorting
            if sorting_settings is None:
                continue
            for axis, settings in zip(SortAxis, [sorting_settings.column, sorting_settings.row]):
                if settings is not None:
                    self._sort_by_measure(axis, pivot_item.pivot_item_id, settings)
