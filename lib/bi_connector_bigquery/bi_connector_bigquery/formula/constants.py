from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class BigQueryDialect(DialectNamespace):
    BIGQUERY = simple_combo(name=DialectName.BIGQUERY)
