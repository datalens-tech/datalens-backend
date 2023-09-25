from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    TypeVar,
)

import attr

from dl_api_lib.pivot.base.data_frame import PivotDataFrame


if TYPE_CHECKING:
    from dl_api_lib.pivot.primitives import (
        MeasureValues,
        PivotHeader,
    )


_PIVOT_DATA_FRAME_TV = TypeVar("_PIVOT_DATA_FRAME_TV", bound="EmptyPivotDataFrame")


@attr.s
class EmptyPivotDataFrame(PivotDataFrame):
    def iter_columns(self) -> Generator[PivotHeader, None, None]:
        yield from ()

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        yield from ()

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        yield from ()

    def clone(self: _PIVOT_DATA_FRAME_TV, **kwargs: Any) -> _PIVOT_DATA_FRAME_TV:
        return attr.evolve(self, **kwargs)

    def get_column_count(self) -> int:
        return 0

    def get_row_count(self) -> int:
        return 0
