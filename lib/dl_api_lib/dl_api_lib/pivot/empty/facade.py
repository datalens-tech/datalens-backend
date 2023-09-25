from __future__ import annotations

import attr

from dl_api_lib.pivot.base.facade import TableDataFacade
from dl_api_lib.pivot.empty.data_frame import EmptyPivotDataFrame
from dl_api_lib.pivot.empty.paginator import EmptyPivotPaginator
from dl_api_lib.pivot.empty.sorter import EmptyPivotSorter


@attr.s
class EmptyDataFrameFacade(TableDataFacade):
    def _make_pivot_dframe(self) -> EmptyPivotDataFrame:
        return EmptyPivotDataFrame()

    def _make_sorter(self) -> EmptyPivotSorter:
        return EmptyPivotSorter(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_dframe=self._pivot_dframe,
        )

    def _make_paginator(self) -> EmptyPivotPaginator:
        return EmptyPivotPaginator()
