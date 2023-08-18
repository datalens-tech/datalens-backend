import abc
from typing import Iterable

import attr

from bi_query_processing.legend.field_legend import Legend
from bi_api_lib.query.formalization.pivot_legend import PivotLegend
from bi_api_lib.pivot.table import PivotTable
from bi_api_lib.pivot.hashable_packing import HashableValuePackerBase, FastJsonHashableValuePacker
from bi_query_processing.merging.primitives import MergedQueryDataRow


@attr.s
class PivotTransformer(abc.ABC):
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _cell_packer: HashableValuePackerBase = attr.ib(kw_only=True, factory=FastJsonHashableValuePacker)

    @abc.abstractmethod
    def pivot(self, rows: Iterable[MergedQueryDataRow]) -> PivotTable:
        """
        Turns raw data stream into a pivot table (``PivotTable``) instance
        composed of ``DataCell`` items. Each cell is a 2-tuple containing
        the actual value and a legend reference index that can be used
        to figure out, what properties this value possesses (data type,
        field id, etc.).
        """

        raise NotImplementedError
