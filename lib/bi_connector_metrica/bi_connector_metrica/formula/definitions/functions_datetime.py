import bi_formula.definitions.functions_datetime as base

from bi_connector_metrica.formula.constants import MetricaDialect as D


DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.METRIKAAPI),
    base.FuncDateadd2Unit.for_dialect(D.METRIKAAPI),
    base.FuncDateadd2Number.for_dialect(D.METRIKAAPI),
]
