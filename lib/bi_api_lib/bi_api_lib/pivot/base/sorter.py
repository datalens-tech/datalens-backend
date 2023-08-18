from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import attr
from bi_api_lib.pivot.sort_strategy import DimensionSortValueStrategy, MeasureSortStrategy

if TYPE_CHECKING:
    from bi_query_processing.legend.field_legend import Legend
    from bi_api_lib.query.formalization.pivot_legend import PivotLegend
    from bi_api_lib.pivot.base.data_frame import PivotDataFrame
    from bi_api_lib.pivot.sort_strategy import SortValueStrategy


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

    @abc.abstractmethod
    def sort(self) -> None:
        """
        Order by row and column dimensions. Specs of other fields will be ignored
        Extract ASC/DESC directions from the `OrderByFieldSpec` items.
        `Measure Name` pseudo-dimension is not ordered,
        measure order from the legend is preserved.
        """
        raise NotImplementedError
