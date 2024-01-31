import logging
from typing import (
    TYPE_CHECKING,
    Iterable,
    Sequence,
)

import attr

from dl_constants.enums import PivotRole
from dl_pivot.base.transformer import PivotTransformer
from dl_pivot.native.data_frame import (
    DoublePivotDataKey,
    FlatPivotDataKey,
    NativePivotDataFrame,
)
from dl_pivot.native.facade import NativeDataFrameFacade
from dl_pivot.primitives import (
    DataCell,
    DataCellVector,
)
from dl_pivot.stream_modifiers import (
    DataCellConverter,
    MeasureDataTransposer,
)
from dl_pivot.table import PivotTable
import dl_query_processing.exc as qp_exc
from dl_query_processing.merging.primitives import MergedQueryDataRow


if TYPE_CHECKING:
    from dl_pivot.stream_modifiers import TransposedDataRow


LOGGER = logging.getLogger(__name__)


@attr.s
class NativePivotTransformer(PivotTransformer):
    """Native Python pivot table"""

    def pivot(self, rows: Iterable[MergedQueryDataRow]) -> PivotTable:
        # Prepare data stream
        raw_dcell_stream: Iterable[Sequence[DataCell]] = DataCellConverter(
            rows=rows,
            cell_packer=self._cell_packer,
            legend=self._legend,
            pivot_legend=self._pivot_legend,
        )
        # Transpose measures (turn multiple measure fields into a single pseudo-field).
        transp_stream: Iterable[TransposedDataRow] = MeasureDataTransposer(
            dcell_stream=raw_dcell_stream,
            pivot_legend=self._pivot_legend,
            cell_packer=self._cell_packer,
        )

        column_piids = [
            item.pivot_item_id for idx, item in enumerate(self._pivot_legend.list_for_role(PivotRole.pivot_column))
        ]
        row_piids = [
            item.pivot_item_id for idx, item in enumerate(self._pivot_legend.list_for_role(PivotRole.pivot_row))
        ]

        dim_piid_set = set(column_piids + row_piids)

        data: dict[DoublePivotDataKey, DataCellVector] = {}
        row_key_set: set[FlatPivotDataKey] = set()
        column_key_set: set[FlatPivotDataKey] = set()
        for dim_vectors, value_vector in transp_stream:
            dim_values: dict[str, DataCellVector] = {}
            for dim_vector in dim_vectors:
                base_dim_cell = dim_vector.cells[0]
                dim_values[base_dim_cell.pivot_item_id] = dim_vector  # type: ignore  # 2024-01-24 # TODO: Invalid index type "int" for "dict[str, DataCellVector]"; expected type "str"  [index]

            if set(dim_values) != dim_piid_set:
                raise qp_exc.PivotUnevenDataColumnsError(
                    f"Expected pivot items{sorted(dim_piid_set)}; got: {sorted(dim_values)}"
                )

            row_key = FlatPivotDataKey(values=tuple(dim_values[piid] for piid in row_piids))  # type: ignore  # 2024-01-24 # TODO: Invalid index type "int" for "dict[str, DataCellVector]"; expected type "str"  [index]
            column_key = FlatPivotDataKey(values=tuple(dim_values[piid] for piid in column_piids))  # type: ignore  # 2024-01-24 # TODO: Invalid index type "int" for "dict[str, DataCellVector]"; expected type "str"  [index]
            row_key_set.add(row_key)
            column_key_set.add(column_key)
            double_key = DoublePivotDataKey(row_key=row_key, column_key=column_key)
            data[double_key] = value_vector  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "DataCellVector | None", target has type "DataCellVector")  [assignment]

        pivot_dframe = NativePivotDataFrame(
            data=data,
            row_keys=list(row_key_set),
            column_keys=list(column_key_set),
        )
        facade = NativeDataFrameFacade(
            raw_pivot_dframe=pivot_dframe,
            legend=self._legend,
            pivot_legend=self._pivot_legend,
        )
        table = PivotTable(
            facade=facade,
            pivot_legend=self._pivot_legend,
            cell_packer=self._cell_packer,
        )
        return table
