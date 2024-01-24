from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import attr

from dl_api_lib.pivot.primitives import (
    PivotHeaderRole,
    SortAxis,
)
from dl_api_lib.pivot.sort_strategy import (
    DimensionSortValueStrategy,
    MeasureSortStrategy,
)
import dl_query_processing.exc as exc


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.data_frame import PivotDataFrame
    from dl_api_lib.pivot.pivot_legend import PivotLegend
    from dl_api_lib.pivot.sort_strategy import SortValueStrategy
    from dl_query_processing.legend.field_legend import Legend


@attr.s
class PivotSorter(abc.ABC):
    _pivot_dframe: PivotDataFrame = attr.ib(kw_only=True)
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _dimension_sort_strategy: SortValueStrategy = attr.ib(kw_only=True)
    _measure_sort_strategy: SortValueStrategy = attr.ib(kw_only=True)

    @_dimension_sort_strategy.default
    def _make_dimension_sort_strategy(self) -> SortValueStrategy:
        return DimensionSortValueStrategy(legend=self._legend, pivot_legend=self._pivot_legend)

    @_measure_sort_strategy.default
    def _make_measure_sort_strategy(self) -> SortValueStrategy:
        return MeasureSortStrategy(legend=self._legend, pivot_legend=self._pivot_legend)

    @staticmethod
    def _complementary_axis(axis: SortAxis) -> SortAxis:
        return next(iter(set(SortAxis) - {axis}))

    def _axis_has_total(self, axis: SortAxis) -> bool:
        headers = self._pivot_dframe.iter_axis_headers(axis)
        total_count = sum(header.info.role_spec.role == PivotHeaderRole.total for header in headers)
        if total_count > 1:
            raise exc.PivotSortingWithSubtotalsIsNotAllowed()
        return bool(total_count)

    @abc.abstractmethod
    def sort(self) -> None:
        """
        Order by row and column dimensions. Specs of other fields will be ignored
        Extract ASC/DESC directions from the `OrderByFieldSpec` items.
        `Measure Name` pseudo-dimension is not ordered,
        measure order from the legend is preserved.
        """
        raise NotImplementedError
