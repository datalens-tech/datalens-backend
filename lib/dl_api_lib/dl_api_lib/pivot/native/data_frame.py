from __future__ import annotations

from typing import (
    Generator,
    NamedTuple,
    TypeVar,
)

import attr

from dl_api_lib.pivot.base.data_frame import PivotDataFrame
from dl_api_lib.pivot.primitives import (
    DataCellVector,
    MeasureValues,
    PivotHeader,
)


class FlatPivotDataKey(NamedTuple):
    values: tuple[DataCellVector, ...]


class DoublePivotDataKey(NamedTuple):
    row_key: FlatPivotDataKey
    column_key: FlatPivotDataKey


_PIVOT_DATA_FRAME_TV = TypeVar("_PIVOT_DATA_FRAME_TV", bound="NativePivotDataFrame")  # TODO: Replace with `Self`


@attr.s
class NativePivotDataFrame(PivotDataFrame):
    data: dict[DoublePivotDataKey, DataCellVector] = attr.ib(kw_only=True)
    row_keys: list[FlatPivotDataKey] = attr.ib(kw_only=True)
    column_keys: list[FlatPivotDataKey] = attr.ib(kw_only=True)

    def iter_columns(self) -> Generator[PivotHeader, None, None]:
        for column_key in self.column_keys:
            yield PivotHeader(values=column_key.values, info=self.headers_info[column_key.values])

    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        for row_key in self.row_keys:
            yield PivotHeader(values=row_key.values, info=self.headers_info[row_key.values])

    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        for row_key in self.row_keys:
            measure_values = tuple(
                self.data.get(DoublePivotDataKey(row_key=row_key, column_key=column_key))
                for column_key in self.column_keys
            )
            row_header = PivotHeader(values=row_key.values, info=self.headers_info[row_key.values])
            yield row_header, measure_values

    def get_column_count(self) -> int:
        return len(self.column_keys)

    def get_row_count(self) -> int:
        return len(self.row_keys)
