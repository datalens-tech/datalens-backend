import dl_formula.definitions.operators_ternary as base

from dl_connector_trino.formula.constants import TrinoDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.TRINO),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.TRINO),
]
