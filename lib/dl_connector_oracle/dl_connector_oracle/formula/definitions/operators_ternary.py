import dl_formula.definitions.operators_ternary as base

from dl_connector_oracle.formula.constants import OracleDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.ORACLE),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.ORACLE),
]
