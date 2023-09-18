import dl_formula.definitions.operators_ternary as base

from bi_connector_mssql.formula.constants import MssqlDialect as D

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.MSSQLSRV),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.MSSQLSRV),
]
