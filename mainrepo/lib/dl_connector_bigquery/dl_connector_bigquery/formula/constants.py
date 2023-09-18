from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)

DIALECT_NAME_BIGQUERY = DialectName.declare("BIGQUERY")


class BigQueryDialect(DialectNamespace):
    BIGQUERY = simple_combo(name=DIALECT_NAME_BIGQUERY)
