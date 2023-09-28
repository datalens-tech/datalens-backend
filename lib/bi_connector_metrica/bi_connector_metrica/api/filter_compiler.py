from dl_query_processing.compilation.filter_compiler import (
    FilterParams,
    MainFilterFormulaCompiler,
)


class MetricaApiFilterFormulaCompiler(MainFilterFormulaCompiler):
    """Does not support datetime casting used for most other sources"""

    def _mangle_date_filter(self, filter_params: FilterParams) -> FilterParams:
        return filter_params
