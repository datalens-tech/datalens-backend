"""
Module defines common logic of sorting values in pivot tables.
This is independent from the specific pivot table implementations.
"""

from __future__ import annotations

import abc
from enum import (
    Enum,
    auto,
    unique,
)
from math import isnan
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Union,
)

import attr

from dl_api_lib.pivot.primitives import (
    DataCellVector,
    MeasureNameValue,
)
from dl_api_lib.pivot.sort_helpers import (
    NORMAL_LEAST_NULL_VALUE,
    TOTAL_GREATEST_NULL_VALUE,
    TOTAL_LEAST_NULL_VALUE,
)
from dl_constants.enums import (
    FieldRole,
    OrderDirection,
    PivotRole,
    UserDataType,
)


if TYPE_CHECKING:
    from dl_api_lib.pivot.pivot_legend import PivotLegend
    from dl_query_processing.legend.field_legend import Legend


@unique
class SortAxis(Enum):
    columns = auto()
    rows = auto()


_PD_AXIS_MAP = {
    SortAxis.columns: 1,
    SortAxis.rows: 0,
}


@attr.s
class SortValueNormalizer(abc.ABC):
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _pivot_item_id: int = attr.ib(kw_only=True)  # To be replaced with pivot_item_id
    _direction: OrderDirection = attr.ib()

    @abc.abstractmethod
    def normalize_vector_value(self, vector: DataCellVector | float | None) -> Any:
        raise NotImplementedError


@attr.s
class BaseSortValueNormalizer(SortValueNormalizer):
    _legend_item_ids: set[int] = attr.ib(init=False)
    _value_converter: Optional[Callable[[Any], Any]] = attr.ib(init=False)

    @_legend_item_ids.default
    def _make_legend_item_ids(self) -> set[int]:
        liid_list = self._pivot_legend.get_item(self._pivot_item_id).legend_item_ids
        legend_items = [self._legend.get_item(legend_item_id) for legend_item_id in liid_list]
        legend_items = [item for item in legend_items if item.role_spec.role != FieldRole.template]
        return {item.legend_item_id for item in legend_items}

    @_value_converter.default
    def _make_value_converter(self) -> Optional[Callable[[Any], Any]]:
        data_types = {self._legend.get_item(legend_item_id).data_type for legend_item_id in self._legend_item_ids}
        assert len(data_types) == 1, "Only single data type is supported within a pivot dimension"
        data_type = next(iter(data_types))
        # Normalize numbers for correct sorting
        if data_type is UserDataType.integer:
            return int
        if data_type is UserDataType.float:
            return float

        return None

    def normalize_vector_value(self, vector: DataCellVector | float | None) -> Any:
        # the case of NaN values (in pandas they represent missing cells)
        if vector is None or isinstance(vector, float) and isnan(vector):
            return NORMAL_LEAST_NULL_VALUE

        assert isinstance(vector, DataCellVector)
        value = vector.main_value
        if value is None:
            # Control how `None`/`null` is sorted
            return NORMAL_LEAST_NULL_VALUE

        if self._value_converter is not None:
            return self._value_converter(value)

        if isinstance(value, str):
            # Casefold strings for case-insensitive sorting
            return value.casefold()

        return value


@attr.s
class MeasureSortValueNormalizer(BaseSortValueNormalizer):
    pass


@attr.s
class DimensionSortValueNormalizer(BaseSortValueNormalizer):
    def normalize_vector_value(self, vector: DataCellVector | float | None) -> Any:
        assert isinstance(vector, DataCellVector)
        legend_item_id = vector.main_legend_item_id
        if legend_item_id not in self._legend_item_ids:
            # Control the sorting of values that are not part of the main data
            # (e.g. template labels for totals).
            # The data type of these values may be different from the main data.
            if self._direction == OrderDirection.asc:
                return TOTAL_GREATEST_NULL_VALUE
            else:  # desc
                # Sorting is reversed, but totals still must be at the end,
                # even after regular NULLs.
                # So a `LeastNullValue` value with a smaller weight is used.
                return TOTAL_LEAST_NULL_VALUE

        return super().normalize_vector_value(vector)


@attr.s
class MeasureNameSortValueNormalizer(SortValueNormalizer):
    _measure_name_order_values: Dict[MeasureNameValue, int] = attr.ib(init=False)

    @_measure_name_order_values.default
    def _make_measure_name_order_values(self) -> Dict[MeasureNameValue, int]:
        return {
            MeasureNameValue(value=item.title, measure_piid=item.pivot_item_id): order_value
            for order_value, item in enumerate(self._pivot_legend.list_for_role(role=PivotRole.pivot_measure))
        }

    def normalize_vector_value(self, vector: DataCellVector | float | None) -> Any:
        assert isinstance(vector, DataCellVector)
        main_value = vector.main_value
        assert isinstance(main_value, MeasureNameValue)
        return self._measure_name_order_values[main_value]


@attr.s
class SortValueStrategy(abc.ABC):
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)

    @abc.abstractmethod
    def get_normalizer(self, pivot_item_id: int, direction: OrderDirection) -> SortValueNormalizer:
        """
        Return a `SortValueNormalizer` instance for the specific type of value.
        The normalizer will adapt the value for sorting taking into account its data type,
        legend role and pivot role.
        """
        raise NotImplementedError


@attr.s
class DimensionSortValueStrategy(SortValueStrategy):
    def get_normalizer(self, pivot_item_id: int, direction: OrderDirection) -> SortValueNormalizer:
        measure_name_pivot_item_ids = self._pivot_legend.get_measure_name_pivot_item_ids()
        if pivot_item_id in measure_name_pivot_item_ids:
            return MeasureNameSortValueNormalizer(
                legend=self._legend,
                pivot_legend=self._pivot_legend,
                pivot_item_id=pivot_item_id,
                direction=direction,
            )

        return DimensionSortValueNormalizer(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_item_id=pivot_item_id,
            direction=direction,
        )


@attr.s
class MeasureSortStrategy(SortValueStrategy):
    def get_normalizer(self, pivot_item_id: int, direction: OrderDirection) -> SortValueNormalizer:
        return MeasureSortValueNormalizer(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_item_id=pivot_item_id,
            direction=direction,
        )
