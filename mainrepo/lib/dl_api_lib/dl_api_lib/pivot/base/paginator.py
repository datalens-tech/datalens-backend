from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Optional,
)


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.data_frame import PivotDataFrame


class PivotPaginator(abc.ABC):
    @abc.abstractmethod
    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        raise NotImplementedError
