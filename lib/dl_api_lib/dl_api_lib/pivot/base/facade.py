from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Optional,
    TypeVar,
)

import attr

from dl_api_lib.pivot.primitives import (
    DataCell,
    DataCellVector,
)
from dl_constants.enums import PivotRole


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.data_frame import PivotDataFrame
    from dl_api_lib.pivot.base.paginator import PivotPaginator
    from dl_api_lib.pivot.base.sorter import PivotSorter
    from dl_api_lib.pivot.pivot_legend import PivotLegend
    from dl_api_lib.pivot.primitives import (
        MeasureValues,
        PivotHeader,
    )
    from dl_query_processing.legend.field_legend import Legend


_FACADE_TV = TypeVar("_FACADE_TV", bound="TableDataFacade")


@attr.s
class TableDataFacade(abc.ABC):
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _pivot_dframe: PivotDataFrame = attr.ib(init=False)
    _sorter: PivotSorter = attr.ib(init=False)
    _paginator: PivotPaginator = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._pivot_dframe = self._make_pivot_dframe()
        self._pivot_dframe.populate_headers_info(self._legend)
        self._sorter = self._make_sorter()
        self._paginator = self._make_paginator()

    @property
    def pivot_dframe(self) -> PivotDataFrame:
        return self._pivot_dframe

    @abc.abstractmethod
    def _make_pivot_dframe(self) -> PivotDataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_sorter(self) -> PivotSorter:
        raise NotImplementedError

    @abc.abstractmethod
    def _make_paginator(self) -> PivotPaginator:
        raise NotImplementedError

    def iter_column_headers(self) -> Generator[PivotHeader, None, None]:
        return self._pivot_dframe.iter_column_headers()

    def iter_row_dim_headers(self) -> Generator[DataCellVector, None, None]:
        dname_legend_item_id = next(iter(self._legend.get_dimension_name_legend_item_ids()))
        dname_pivot_item_id = next(iter(self._pivot_legend.get_dimension_name_pivot_item_ids()))
        for row_dim_item in self._pivot_legend.list_for_role(PivotRole.pivot_row):
            yield DataCellVector(
                cells=(
                    DataCell(
                        value=row_dim_item.title,
                        legend_item_id=dname_legend_item_id,
                        pivot_item_id=dname_pivot_item_id,
                    ),
                )
            )

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        return self._pivot_dframe.iter_rows()

    def get_column_count(self) -> int:
        return self._pivot_dframe.get_column_count()

    def get_row_count(self) -> int:
        return self._pivot_dframe.get_row_count()

    def clone(self: _FACADE_TV, **kwargs: Any) -> _FACADE_TV:
        return attr.evolve(self, **kwargs)

    def sort(self) -> None:
        self._sorter.sort()

    def paginate(self, offset_rows: Optional[int], limit_rows: Optional[int]) -> None:
        self._pivot_dframe = self._paginator.paginate(
            pivot_dframe=self._pivot_dframe,
            offset_rows=offset_rows,
            limit_rows=limit_rows,
        )
        self._sorter = self._make_sorter()
