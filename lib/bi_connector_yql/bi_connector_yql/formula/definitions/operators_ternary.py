import dl_formula.definitions.operators_ternary as base

from bi_connector_yql.formula.constants import YqlDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.YQL),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.YQL),
]
