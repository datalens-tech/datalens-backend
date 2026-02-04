from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    from dl_pivot.base.data_frame import PivotDataFrame


class PivotPaginator(abc.ABC):
    @abc.abstractmethod
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: int | None = None,
        offset_rows: int | None = None,
    ) -> PivotDataFrame:
        raise NotImplementedError
