from __future__ import annotations

from typing import (
    Generator,
    Optional,
)

import attr
import pandas as pd

from dl_pivot.base.data_frame import PivotDataFrame
from dl_pivot.primitives import (
    DataCellVector,
    MeasureValues,
    PivotHeader,
)


@attr.s
class PdPivotDataFrame(PivotDataFrame):
    """
    A simple wrapper for `pandas.DataFrame`
    that simplifies iteration over columns and rows a little.
    """

    _pd_df: pd.DataFrame = attr.ib(kw_only=True)

    @property
    def pd_df(self) -> pd.DataFrame:
        return self._pd_df

    def iter_column_headers(self) -> Generator[PivotHeader, None, None]:
        for values in self._pd_df.columns:
            if isinstance(values, DataCellVector):
                # Single column dimension, so normalize
                values = (values,)


            yield PivotHeader(values=values, info=self.headers_info[values])  # type: ignore # TODO: Argument "values" to "PivotHeader" has incompatible type "str"

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        for values in self._pd_df.index:
            if isinstance(values, DataCellVector):
                # Single row dimension, so normalize
                values = (values,)

            yield PivotHeader(values=values, info=self.headers_info[values])

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        headers = self.iter_row_headers()
        for row in self._pd_df.itertuples(index=False):
            values: tuple[Optional[DataCellVector], ...] = tuple(
                (cell if isinstance(cell, DataCellVector) else None) for cell in row
            )
            yield next(headers), values

    def get_column_count(self) -> int:
        return len(self._pd_df.columns)

    def get_row_count(self) -> int:
        return len(self._pd_df.index)


@attr.s
class PdSeriesPivotDataFrameBase(PivotDataFrame):
    """
    A base for DataFrame wrappers for `pandas.Series`
    """

    _pd_series: pd.Series = attr.ib(kw_only=True)

    @property
    def pd_series(self) -> pd.Series:
        return self._pd_series

    def _iter_headers(self) -> Generator[PivotHeader, None, None]:
        for values in self._pd_series.index:
            if isinstance(values, DataCellVector):
                # Single column dimension, so normalize
                values = (values,)

            yield PivotHeader(values=values, info=self.headers_info[values])

    def _get_values(self) -> MeasureValues:
        values: tuple[Optional[DataCellVector], ...] = tuple(
            (value if isinstance(value, DataCellVector) else None) for value in self._pd_series.values
        )
        return values


@attr.s
class PdHSeriesPivotDataFrame(PdSeriesPivotDataFrameBase):
    """
    A horizontal `pandas.Series`
    """

    def iter_column_headers(self) -> Generator[PivotHeader, None, None]:
        yield from self._iter_headers()

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        yield PivotHeader()

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        yield PivotHeader(), self._get_values()

    def get_column_count(self) -> int:
        return len(self._pd_series)

    def get_row_count(self) -> int:
        return 1


@attr.s
class PdVSeriesPivotDataFrame(PdSeriesPivotDataFrameBase):
    """
    A vertical `pandas.Series`
    """

    def iter_column_headers(self) -> Generator[PivotHeader, None, None]:
        yield PivotHeader()

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        yield from self._iter_headers()

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        for header, value in zip(self._iter_headers(), self._get_values(), strict=True):
            yield header, (value,)

    def get_column_count(self) -> int:
        return 1

    def get_row_count(self) -> int:
        return len(self._pd_series)
