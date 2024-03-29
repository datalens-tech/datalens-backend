from __future__ import annotations

import abc
from itertools import count
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Sequence,
    Union,
    cast,
)

import pandas as pd

from dl_constants.enums import (
    OrderDirection,
    PivotRole,
)
from dl_pivot.base.sorter import PivotSorter
from dl_pivot.pivot_legend import (
    PivotDimensionRoleSpec,
    PivotMeasureRoleSpec,
)
from dl_pivot.primitives import (
    DataCellVector,
    SortAxis,
)
from dl_pivot_pandas.pandas.data_frame import (
    PdHSeriesPivotDataFrame,
    PdPivotDataFrame,
    PdVSeriesPivotDataFrame,
)
import dl_query_processing.exc as exc


if TYPE_CHECKING:
    from dl_pivot.primitives import PivotMeasureSortingSettings


_PD_AXIS_MAP = {
    SortAxis.columns: 1,
    SortAxis.rows: 0,
}


class PdPivotSorterBase(PivotSorter):
    """
    pd.DataFrame-specific sorter.
    """

    @abc.abstractmethod
    def _get_pd_obj(self) -> Union[pd.DataFrame, pd.Series]:
        raise NotImplementedError

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

    @abc.abstractmethod
    def _get_pd_axis(self, axis: SortAxis) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_sorting_key(self, sorting_idx: int, axis: SortAxis) -> pd.Series:
        raise NotImplementedError

    @abc.abstractmethod
    def _apply_sorting_key(self, axis: SortAxis, sorting_key: pd.Series) -> None:
        raise NotImplementedError

    def _single_axis_sort(self, axis: SortAxis, directions: Sequence[OrderDirection]) -> None:
        ascending_list = [direction == OrderDirection.asc for direction in directions]
        ascending: Union[bool, list[bool]] = ascending_list  # Separate var to avoid typing errors
        if len(ascending_list) == 1:
            # If there is only one dimension, then it must be a scalar
            ascending = ascending_list[0]

        idx_iter = count()  # see the `dimension_idx` function below

        def dimension_idx() -> int:
            """
            Returns the index of the current dimension along the current axis.
            The first call returns 0, the second - 1, and so on.
            """
            return next(idx_iter)

        def sorting_key_func(idx: pd.Index) -> pd.Index:
            """Convert cell vectors in data frame index to simple orderable values"""
            idx_item = idx[0]
            assert isinstance(idx_item, DataCellVector)
            legend_item_id = idx_item.main_legend_item_id
            pivot_item_ids = self._pivot_legend.leg_item_id_to_pivot_item_id_list(legend_item_id=legend_item_id)
            # FIXME: Find a way to determine which of the several possible pivot items this really is
            pivot_item_id = next(iter(pivot_item_ids))
            dim_idx = dimension_idx()
            direction = directions[dim_idx]
            normalizer = self._dimension_sort_strategy.get_normalizer(
                pivot_item_id=pivot_item_id,
                direction=direction,
            )

            return pd.Index([normalizer.normalize_vector_value(vector) for vector in idx])

        self._get_pd_obj().sort_index(  # type: ignore  # 2024-01-24 # TODO: No overload variant of "sort_index" of "DataFrame" matches argument types "int", "bool | list[bool]", "bool", "Callable[[Index], Index]"  [call-overload]
            axis=self._get_pd_axis(axis),
            ascending=ascending,
            inplace=True,
            key=sorting_key_func,
        )

    def _sort_by_measure(self, axis: SortAxis, sorting_piid: int, settings: PivotMeasureSortingSettings) -> None:
        sorting_idx: Optional[int] = None

        for idx, header in enumerate(self._pivot_dframe.iter_axis_headers(axis)):
            if header.compare_sorting_settings(settings):
                if sorting_idx is None:
                    sorting_idx = idx
                    header.info.sorting_direction = settings.direction
                else:  # should never actually occur, as header_values + role_spec uniquely identify sorting_idx
                    raise exc.PivotSortingRowOrColumnIsAmbiguous()
        if sorting_idx is None:
            raise exc.PivotSortingRowOrColumnNotFound()

        # we can use sort_values to sort a DataFrame by row or column,
        # however tuples in indexes make it incredibly hard to apply,
        # so instead we find a row/column by index, get a sorting permutation
        # using argsort and apply it to the DataFrame
        sorting_key = self._get_sorting_key(sorting_idx, axis)
        normalizer = self._measure_sort_strategy.get_normalizer(
            pivot_item_id=sorting_piid, direction=settings.direction
        )
        sorting_key = sorting_key.map(normalizer.normalize_vector_value).values.argsort()  # type: ignore # TODO: Incompatible types in assignment
        if settings.direction == OrderDirection.desc:
            sorting_key = sorting_key[::-1]
        if self._axis_has_total(self._complementary_axis(axis)):
            # manually put the total at the end
            key_len = len(sorting_key)
            total_pos = list(sorting_key).index(key_len - 1)
            order = list(range(key_len))
            order.append(order.pop(total_pos))
            sorting_key = sorting_key[order]  # type: ignore  # 2024-01-24 # TODO: Invalid index type "list[int]" for "Series[Any]"; expected type "list[str] | Index | Series[Any] | slice | Series[bool] | ndarray[Any, dtype[bool_]] | list[bool] | tuple[Any | slice, ...]"  [index]
        self._apply_sorting_key(axis, sorting_key)

    def sort(self) -> None:
        # sort by measure; overrides provide additional sorting by dimensions
        for pivot_item in self._pivot_legend.list_for_role(role=PivotRole.pivot_measure):
            sorting_settings = cast(PivotMeasureRoleSpec, pivot_item.role_spec).sorting
            if sorting_settings is None:
                continue
            for axis, settings in zip(
                [SortAxis.columns, SortAxis.rows],
                [sorting_settings.column, sorting_settings.row],
                strict=True,
            ):
                if settings is not None:
                    self._sort_by_measure(axis, pivot_item.pivot_item_id, settings)


class PdPivotSorter(PdPivotSorterBase):
    """
    pd.DataFrame-specific sorter.
    """

    def _get_pd_obj(self) -> pd.DataFrame:
        assert isinstance(self._pivot_dframe, PdPivotDataFrame)
        return self._pivot_dframe.pd_df

    def _get_pd_axis(self, axis: SortAxis) -> int:
        return _PD_AXIS_MAP[axis]

    def _get_sorting_key(self, sorting_idx: int, axis: SortAxis) -> pd.Series:
        if axis == SortAxis.columns:
            return self._get_pd_obj().iloc[:, sorting_idx]
        return self._get_pd_obj().iloc[sorting_idx]

    def _apply_sorting_key(self, axis: SortAxis, sorting_key: pd.Series) -> None:
        if axis == SortAxis.columns:
            new_df = self._get_pd_obj().iloc[sorting_key]  # ordering by column, so reorder rows
        else:
            new_df = self._get_pd_obj().iloc[:, sorting_key]  # ordering by row, so reorder columns
        self._pivot_dframe._pd_df = new_df  # type: ignore  # 2024-01-30 # TODO: "PivotDataFrame" has no attribute "_pd_df"  [attr-defined]

    def sort(self) -> None:
        directions_by_axis = self._resolve_axis_order()
        for axis in (SortAxis.rows, SortAxis.columns):
            if not directions_by_axis[axis]:
                # Nothing to sort here
                continue
            self._single_axis_sort(axis=axis, directions=directions_by_axis[axis])
        super().sort()


class PdSeriesPivotSorterBase(PdPivotSorterBase):
    _AXIS: ClassVar[SortAxis]

    def _get_pd_obj(self) -> pd.Series:
        assert isinstance(self._pivot_dframe, (PdHSeriesPivotDataFrame, PdVSeriesPivotDataFrame))
        return self._pivot_dframe.pd_series

    def _get_pd_axis(self, axis: SortAxis) -> int:
        return 0

    def _get_sorting_key(self, sorting_idx: int, axis: SortAxis) -> pd.Series:
        assert axis == self._AXIS
        # returning a sentinel value, as measure sorting in case of series is trivial
        return pd.Series()

    def _apply_sorting_key(self, axis: SortAxis, sorting_key: pd.Series) -> None:
        assert axis == self._AXIS
        # nothing to do here: sorting by one value is trivial

    def sort(self) -> None:
        directions_by_axis = self._resolve_axis_order()
        axis = self._AXIS
        assert not directions_by_axis[self._complementary_axis(axis)]
        self._single_axis_sort(axis=axis, directions=directions_by_axis[axis])
        super().sort()


class PdHSeriesPivotSorter(PdSeriesPivotSorterBase):
    _AXIS = SortAxis.columns


class PdVSeriesPivotSorter(PdSeriesPivotSorterBase):
    _AXIS = SortAxis.rows
