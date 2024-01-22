import abc

import attr

from dl_api_lib.pivot.base.transformer import PivotTransformer
from dl_api_lib.pivot.hashable_packing import (
    FastJsonHashableValuePacker,
    HashableValuePackerBase,
)
from dl_api_lib.pivot.pivot_legend import PivotLegend
from dl_query_processing.legend.field_legend import Legend


@attr.s
class PivotTransformerFactory(abc.ABC):
    _cell_packer: HashableValuePackerBase = attr.ib(kw_only=True, factory=FastJsonHashableValuePacker)

    @abc.abstractmethod
    def get_transformer(self, legend: Legend, pivot_legend: PivotLegend) -> PivotTransformer:
        raise NotImplementedError
