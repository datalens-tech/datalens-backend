from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_YDB = DialectName.declare('YDB')  # YDB ScanQuery connection (YQL dialect)
DIALECT_NAME_YQ = DialectName.declare('YQ')  # YQ (Yandex Query) (YQL dialect)


class YqlDialect(DialectNamespace):
    YDB = simple_combo(name=DIALECT_NAME_YDB)
    YQ = simple_combo(name=DIALECT_NAME_YQ)
    YQL = YDB | YQ
