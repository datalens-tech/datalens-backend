from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

from dl_pivot.base.paginator import PivotPaginator
from dl_pivot.empty.data_frame import EmptyPivotDataFrame


if TYPE_CHECKING:
    from dl_pivot.base.data_frame import PivotDataFrame


class EmptyPivotPaginator(PivotPaginator):
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        assert isinstance(pivot_dframe, EmptyPivotDataFrame)
        return pivot_dframe
