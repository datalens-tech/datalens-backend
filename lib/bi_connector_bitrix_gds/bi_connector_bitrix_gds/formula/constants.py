from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_BITRIX = DialectName.declare('BITRIX')


class BitrixDialect(DialectNamespace):
    BITRIX = simple_combo(name=DIALECT_NAME_BITRIX)
