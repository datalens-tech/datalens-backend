from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_BIGQUERY = DialectName.declare('BIGQUERY')


class BigQueryDialect(DialectNamespace):
    BIGQUERY = simple_combo(name=DIALECT_NAME_BIGQUERY)
