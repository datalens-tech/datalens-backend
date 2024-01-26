from __future__ import annotations

import attr

from dl_pivot.base.facade import TableDataFacade
from dl_pivot.native.data_frame import NativePivotDataFrame
from dl_pivot.native.paginator import NativePivotPaginator
from dl_pivot.native.sorter import NativePivotSorter


@attr.s
class NativeDataFrameFacade(TableDataFacade):
    _raw_pivot_dframe: NativePivotDataFrame = attr.ib(kw_only=True)

    def _make_pivot_dframe(self) -> NativePivotDataFrame:
        return self._raw_pivot_dframe

    def _make_sorter(self) -> NativePivotSorter:
        return NativePivotSorter(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_dframe=self._pivot_dframe,
        )

    def _make_paginator(self) -> NativePivotPaginator:
        return NativePivotPaginator()
