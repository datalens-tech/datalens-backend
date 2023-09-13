from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_GSHEETS = DialectName.declare('GSHEETS')


class GSheetsDialect(DialectNamespace):
    GSHEETS = simple_combo(name=DIALECT_NAME_GSHEETS)
