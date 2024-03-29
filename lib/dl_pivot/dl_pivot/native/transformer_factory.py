import attr

from dl_pivot.base.transformer_factory import PivotTransformerFactory
from dl_pivot.native.transformer import NativePivotTransformer
from dl_pivot.pivot_legend import PivotLegend
from dl_query_processing.legend.field_legend import Legend


@attr.s
class NativePivotTransformerFactory(PivotTransformerFactory):
    def get_transformer(self, legend: Legend, pivot_legend: PivotLegend) -> NativePivotTransformer:
        return NativePivotTransformer(
            legend=legend,
            pivot_legend=pivot_legend,
            cell_packer=self._cell_packer,
        )
