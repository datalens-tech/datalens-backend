from dl_connector_bigquery.formula.constants import BigQueryDialect as D
import dl_formula.definitions.operators_ternary as base

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.BIGQUERY),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.BIGQUERY),
]
