from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_GSHEETS = DialectName.declare("GSHEETS")


class GSheetsDialect(DialectNamespace):
    GSHEETS = simple_combo(name=DIALECT_NAME_GSHEETS)
