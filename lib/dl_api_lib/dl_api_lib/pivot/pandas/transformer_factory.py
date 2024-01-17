import attr

from dl_api_lib.pivot.base.transformer_factory import PivotTransformerFactory
from dl_api_lib.pivot.pandas.transformer import PdPivotTransformer
from dl_api_lib.query.formalization.pivot_legend import PivotLegend
from dl_query_processing.legend.field_legend import Legend


@attr.s
class PdPivotTransformerFactory(PivotTransformerFactory):
    def get_transformer(self, legend: Legend, pivot_legend: PivotLegend) -> PdPivotTransformer:
        return PdPivotTransformer(
            legend=legend,
            pivot_legend=pivot_legend,
            cell_packer=self._cell_packer,
        )
