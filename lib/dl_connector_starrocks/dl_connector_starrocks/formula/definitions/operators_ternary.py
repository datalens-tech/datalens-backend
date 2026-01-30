import dl_formula.definitions.operators_ternary as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.STARROCKS_3_0),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.STARROCKS_3_0),
]
