from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class YqlDialect(DialectNamespace):
    YDB = simple_combo(name=DialectName.YDB)
    YQ = simple_combo(name=DialectName.YQ)
    YQL = YDB | YQ
