from __future__ import annotations

import attr
import pandas as pd

from dl_pivot.base.facade import TableDataFacade
from dl_pivot_pandas.pandas.data_frame import (
    PdHSeriesPivotDataFrame,
    PdPivotDataFrame,
    PdVSeriesPivotDataFrame,
)
from dl_pivot_pandas.pandas.paginator import (
    PdHSeriesPivotPaginator,
    PdPivotPaginator,
    PdVSeriesPivotPaginator,
)
from dl_pivot_pandas.pandas.sorter import (
    PdHSeriesPivotSorter,
    PdPivotSorter,
    PdVSeriesPivotSorter,
)


@attr.s
class PdDataFrameFacade(TableDataFacade):
    _pd_df: pd.DataFrame = attr.ib(kw_only=True)

    def _make_pivot_dframe(self) -> PdPivotDataFrame:
        return PdPivotDataFrame(pd_df=self._pd_df)

    def _make_sorter(self) -> PdPivotSorter:
        return PdPivotSorter(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_dframe=self._pivot_dframe,
        )

    def _make_paginator(self) -> PdPivotPaginator:
        return PdPivotPaginator()


@attr.s
class PdSeriesDataFrameFacadeBase(TableDataFacade):
    _pd_series: pd.Series = attr.ib(kw_only=True)


class PdHSeriesDataFrameFacade(PdSeriesDataFrameFacadeBase):
    def _make_pivot_dframe(self) -> PdHSeriesPivotDataFrame:
        return PdHSeriesPivotDataFrame(pd_series=self._pd_series)

    def _make_sorter(self) -> PdHSeriesPivotSorter:
        return PdHSeriesPivotSorter(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_dframe=self._pivot_dframe,
        )

    def _make_paginator(self) -> PdHSeriesPivotPaginator:
        return PdHSeriesPivotPaginator()


@attr.s
class PdVSeriesDataFrameFacade(PdSeriesDataFrameFacadeBase):
    def _make_pivot_dframe(self) -> PdVSeriesPivotDataFrame:
        return PdVSeriesPivotDataFrame(pd_series=self._pd_series)

    def _make_sorter(self) -> PdVSeriesPivotSorter:
        return PdVSeriesPivotSorter(
            legend=self._legend,
            pivot_legend=self._pivot_legend,
            pivot_dframe=self._pivot_dframe,
        )

    def _make_paginator(self) -> PdVSeriesPivotPaginator:
        return PdVSeriesPivotPaginator()
