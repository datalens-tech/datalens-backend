import dl_formula.definitions.operators_ternary as base

from dl_connector_ydb.formula.constants import YqlDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.YQL),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.YQL),
]
