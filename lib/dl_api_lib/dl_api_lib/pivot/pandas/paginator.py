from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

from dl_api_lib.pivot.base.paginator import PivotPaginator
from dl_api_lib.pivot.pandas.data_frame import (
    PdHSeriesPivotDataFrame,
    PdPivotDataFrame,
    PdVSeriesPivotDataFrame,
)


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.data_frame import PivotDataFrame


class PdPivotPaginator(PivotPaginator):
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        assert isinstance(pivot_dframe, PdPivotDataFrame)
        pd_df = pivot_dframe.pd_df
        total_rows = len(pd_df.index)

        if offset_rows is not None and offset_rows != 0:
            assert offset_rows > 0
            tail_size = total_rows - min(total_rows, offset_rows)
            pd_df = pd_df.tail(tail_size)
            total_rows = tail_size

        if limit_rows is not None:
            assert limit_rows > 0
            pd_df = pd_df.head(min(total_rows, limit_rows))

        # Create table copy with truncated data frame
        pivot_dframe = pivot_dframe.clone(pd_df=pd_df)
        return pivot_dframe


class PdHSeriesPivotPaginator(PivotPaginator):
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        assert isinstance(pivot_dframe, PdHSeriesPivotDataFrame)
        pd_series = pivot_dframe.pd_series
        if limit_rows is not None:
            assert limit_rows > 1
        if offset_rows is not None and offset_rows > 0:
            assert pivot_dframe.get_row_count() == 1
            pd_series = pd_series.transform(lambda x: None)  # type: ignore

        pivot_dframe = pivot_dframe.clone(pd_series=pd_series)
        return pivot_dframe


class PdVSeriesPivotPaginator(PivotPaginator):
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        assert isinstance(pivot_dframe, PdVSeriesPivotDataFrame)
        pd_series = pivot_dframe.pd_series
        total_rows = len(pd_series.index)

        if offset_rows is not None and offset_rows != 0:
            assert offset_rows > 0
            tail_size = total_rows - min(total_rows, offset_rows)
            pd_series = pd_series.tail(tail_size)
            total_rows = tail_size

        if limit_rows is not None:
            assert limit_rows > 0
            pd_series = pd_series.head(min(total_rows, limit_rows))

        # Create table copy with truncated data frame
        pivot_dframe = pivot_dframe.clone(pd_series=pd_series)
        return pivot_dframe
