from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)

DIALECT_NAME_BITRIX = DialectName.declare("BITRIX")


class BitrixDialect(DialectNamespace):
    BITRIX = simple_combo(name=DIALECT_NAME_BITRIX)
