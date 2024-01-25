import attr

from dl_pivot.base.transformer_factory import PivotTransformerFactory
from dl_pivot.pivot_legend import PivotLegend
from dl_pivot_pandas.pandas.transformer import PdPivotTransformer
from dl_query_processing.legend.field_legend import Legend


@attr.s
class PdPivotTransformerFactory(PivotTransformerFactory):
    def get_transformer(self, legend: Legend, pivot_legend: PivotLegend) -> PdPivotTransformer:
        return PdPivotTransformer(
            legend=legend,
            pivot_legend=pivot_legend,
            cell_packer=self._cell_packer,
        )
