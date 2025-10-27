import dl_formula.definitions.functions_native as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.BIGQUERY),
    base.DBCallFloat.for_dialect(D.BIGQUERY),
    base.DBCallString.for_dialect(D.BIGQUERY),
    base.DBCallBool.for_dialect(D.BIGQUERY),
    base.DBCallArrayInt.for_dialect(D.BIGQUERY),
    base.DBCallArrayFloat.for_dialect(D.BIGQUERY),
    base.DBCallArrayString.for_dialect(D.BIGQUERY),
    base.DBCallAggInt.for_dialect(D.BIGQUERY),
    base.DBCallAggFloat.for_dialect(D.BIGQUERY),
    base.DBCallAggString.for_dialect(D.BIGQUERY),
]
