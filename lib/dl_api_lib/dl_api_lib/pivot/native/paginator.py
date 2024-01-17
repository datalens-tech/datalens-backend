from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_lib.pivot.base.paginator import PivotPaginator
from dl_api_lib.pivot.native.data_frame import (
    DoublePivotDataKey,
    FlatPivotDataKey,
    NativePivotDataFrame,
)
from dl_api_lib.pivot.primitives import DataCellVector


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.data_frame import PivotDataFrame


@attr.s
class NativePivotPaginator(PivotPaginator):
    _remove_empty_columns: bool = attr.ib(kw_only=True, default=False)  # See usage for in-depth comment

    def paginate(
        self,
        pivot_dframe: PivotDataFrame,
        limit_rows: Optional[int] = None,
        offset_rows: Optional[int] = None,
    ) -> PivotDataFrame:
        assert isinstance(pivot_dframe, NativePivotDataFrame)

        start = offset_rows
        end = ((offset_rows or 0) + limit_rows) if limit_rows is not None else None

        new_row_keys = pivot_dframe.row_keys[start:end]
        new_column_keys = pivot_dframe.column_keys[:]
        new_data: dict[DoublePivotDataKey, DataCellVector] = {}
        unused_column_keys: set[FlatPivotDataKey] = set(pivot_dframe.column_keys)
        for row_key in new_row_keys:
            for column_key in new_column_keys:
                double_key = DoublePivotDataKey(row_key=row_key, column_key=column_key)
                value = pivot_dframe.data.get(double_key)
                if value is not None:
                    new_data[double_key] = value
                    if column_key in unused_column_keys:
                        # column_key is used in the new data sub-set, so remove it from unused
                        unused_column_keys.remove(column_key)

        if self._remove_empty_columns:
            # If feature is enabled, clean up columns that have been left
            # without any non-empty cells (i.e. all rows with values in this column
            # have been removed by the crop, so the column becomes empty).
            # This might seem logical from a certain standpoint,
            # but the pandas implementation does not do this.
            # So it should be disabled by default.
            for unused_column_key in unused_column_keys:
                new_column_keys.remove(unused_column_key)

        new_pivot_dframe = NativePivotDataFrame(
            data=new_data,
            row_keys=new_row_keys,
            column_keys=new_column_keys,
        )
        return new_pivot_dframe
