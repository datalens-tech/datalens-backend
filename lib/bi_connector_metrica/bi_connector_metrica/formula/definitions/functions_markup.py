import dl_formula.definitions.functions_markup as base

from bi_connector_metrica.formula.constants import MetricaDialect as D

DEFINITIONS_MARKUP = [
    # __str
    base.FuncInternalStrConst.for_dialect(D.METRIKAAPI),
    base.FuncInternalStr.for_dialect(D.METRIKAAPI),
]
