from __future__ import annotations

from collections.abc import Generator
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
)

import attr

from dl_pivot.base.data_frame import PivotDataFrame

if TYPE_CHECKING:
    from dl_pivot.primitives import (
        MeasureValues,
        PivotHeader,
    )


@attr.s
class EmptyPivotDataFrame(PivotDataFrame):
    def iter_column_headers(self) -> Generator[PivotHeader, None, None]:
        yield from ()

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        yield from ()

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        yield from ()

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)

    def get_column_count(self) -> int:
        return 0

    def get_row_count(self) -> int:
        return 0
