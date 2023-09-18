import dl_formula.definitions.functions_string as base

from bi_connector_gsheets.formula.constants import GSheetsDialect as D


DEFINITIONS_STRING = [
    # concat
    base.ConcatMultiStrConst.for_dialect(D.GSHEETS),
]
