from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
import dl_formula.definitions.operators_ternary as base


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.POSTGRESQL),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.POSTGRESQL),
]
