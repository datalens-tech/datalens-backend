from __future__ import annotations

from typing import TYPE_CHECKING

from dl_query_processing.compilation.filter_compiler import MainFilterFormulaCompiler

if TYPE_CHECKING:
    from dl_query_processing.compilation.filter_compiler import FilterParams


class GSheetsFilterFormulaCompiler(MainFilterFormulaCompiler):
    """ connector-specific customizations point """

    def _mangle_date_filter(self, filter_params: FilterParams) -> FilterParams:
        return filter_params  # Disable the datetime mangling
