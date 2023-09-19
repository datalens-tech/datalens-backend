import dl_formula.definitions.operators_ternary as base

from bi_connector_metrica.formula.constants import MetricaDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.METRIKAAPI),
]
