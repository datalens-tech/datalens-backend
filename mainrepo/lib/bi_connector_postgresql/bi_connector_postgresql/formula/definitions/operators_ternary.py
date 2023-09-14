import bi_formula.definitions.operators_ternary as base

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.POSTGRESQL),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.POSTGRESQL),
]
