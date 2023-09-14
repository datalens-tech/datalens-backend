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

from bi_api_lib.pivot.primitives import (
    DataCellVector,
    MeasureNameValue,
)
from bi_constants.enums import (
    BIType,
    FieldRole,
    OrderDirection,
    PivotRole,
)

if TYPE_CHECKING:
    from bi_api_lib.query.formalization.pivot_legend import PivotLegend
    from bi_query_processing.legend.field_legend import Legend


@unique
class SortAxis(Enum):
    columns = auto()
    rows = auto()


_PD_AXIS_MAP = {
    SortAxis.columns: 1,
    SortAxis.rows: 0,
}


class OrderableNullValueBase(abc.ABC):
    """
    An orderable, hashable and immutable value that can be used
    instead of ``None`` when sorting something.
    """

    __slots__ = ("weight",)

    weight: int

    def __init__(self, weight: int = 0):
        super().__setattr__("weight", weight)

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __hash__(self) -> int:
        return 0

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and other.weight == self.weight

    def __ne__(self, other: Any) -> bool:
        return type(other) is not type(self) or other.weight != self.weight

    @abc.abstractmethod
    def __gt__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __ge__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __lt__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __le__(self, other: Any) -> bool:
        raise NotImplementedError()


class LeastNullValue(OrderableNullValueBase):
    """A null/None placeholder that is less than everything else."""

    __slots__ = ()

    def __gt__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight > other.weight

    def __ge__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight >= other.weight

    def __lt__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight < other.weight

    def __le__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight <= other.weight


class GreatestNullValue(OrderableNullValueBase):
    """A null/None placeholder that is greater than everything else."""

    __slots__ = ()

    def __gt__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight > other.weight

    def __ge__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight >= other.weight

    def __lt__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight < other.weight

    def __le__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight <= other.weight


NORMAL_LEAST_NULL_VALUE = LeastNullValue()
TOTAL_GREATEST_NULL_VALUE = GreatestNullValue(1)
TOTAL_LEAST_NULL_VALUE = LeastNullValue(-1)


@attr.s
class SortValueNormalizer(abc.ABC):
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _pivot_item_id: int = attr.ib(kw_only=True)  # To be replaced with pivot_item_id
    _direction: OrderDirection = attr.ib()

    @abc.abstractmethod
    def normalize_vector_value(self, vector: Union[DataCellVector, float]) -> Any:
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
        if data_type is BIType.integer:
            return int
        if data_type is BIType.float:
            return float

        return None

    def normalize_vector_value(self, vector: Union[DataCellVector, float]) -> Any:
        # the case of NaN values
        if isinstance(vector, float) and isnan(vector):
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
    def normalize_vector_value(self, vector: Union[DataCellVector, float]) -> Any:
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

    def normalize_vector_value(self, vector: Union[DataCellVector, float]) -> Any:
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
