import dl_formula.definitions.operators_unary as base

from dl_connector_metrica.formula.constants import MetricaDialect as D


DEFINITIONS_UNARY = [
    # not
    base.UnaryNotBool.for_dialect(D.METRIKAAPI),
]
