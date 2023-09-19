import dl_formula.definitions.functions_markup as base

from bi_connector_bitrix_gds.formula.constants import BitrixDialect as D


DEFINITIONS_MARKUP = [
    # __str
    base.FuncInternalStrConst.for_dialect(D.BITRIX),
    base.FuncInternalStr.for_dialect(D.BITRIX),
]
