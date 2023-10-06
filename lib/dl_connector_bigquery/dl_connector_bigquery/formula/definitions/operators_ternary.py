import dl_formula.definitions.operators_ternary as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.BIGQUERY),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.BIGQUERY),
]
