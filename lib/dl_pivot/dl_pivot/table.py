from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Hashable,
)

import attr

from dl_pivot.pivot_legend import PivotLegend
from dl_pivot.primitives import (
    DataRow,
    MeasureNameValue,
)
from dl_query_processing.postprocessing.primitives import PostprocessedValue


if TYPE_CHECKING:
    from dl_pivot.base.facade import TableDataFacade
    from dl_pivot.hashable_packing import HashableValuePackerBase
    from dl_pivot.primitives import (
        DataCellVector,
        PivotHeader,
    )


@attr.s
class PivotTable:
    _facade: TableDataFacade = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _cell_packer: HashableValuePackerBase = attr.ib(kw_only=True)

    @property
    def pivot_legend(self) -> PivotLegend:
        return self._pivot_legend

    @property
    def facade(self) -> TableDataFacade:
        return self._facade

    def get_columns(self) -> list[PivotHeader]:
        return [header for header in self.facade.iter_column_headers()]

    def get_row_dim_headers(self) -> list[DataCellVector]:
        return [header for header in self.facade.iter_row_dim_headers()]

    def get_rows(self) -> Generator[DataRow, None, None]:
        for header, values in self.facade.iter_rows():
            yield DataRow(header=header, values=values)

    def get_column_count(self) -> int:
        return self._facade.get_column_count()

    def get_row_count(self) -> int:
        return self._facade.get_row_count()

    def unpack_value(self, hashable_value: Hashable) -> PostprocessedValue:
        if isinstance(hashable_value, MeasureNameValue):
            hashable_value = hashable_value.value
        return self._cell_packer.unpack(hashable_value)

    def clone(self, **kwargs: Any) -> PivotTable:
        return attr.evolve(self, **kwargs)
