import bi_formula.definitions.functions_markup as base

from bi_connector_gsheets.formula.constants import GSheetsDialect as D


DEFINITIONS_MARKUP = [
    # __str
    base.FuncInternalStrConst.for_dialect(D.GSHEETS),
    base.FuncInternalStr.for_dialect(D.GSHEETS),
]
