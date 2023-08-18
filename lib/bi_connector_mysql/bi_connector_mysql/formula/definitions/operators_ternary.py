import bi_formula.definitions.operators_ternary as base

from bi_connector_mysql.formula.constants import MySQLDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.MYSQL),

    # notbetween
    base.TernaryNotBetween.for_dialect(D.MYSQL),
]
