import dl_formula.definitions.functions_datetime as base

from bi_connector_bitrix_gds.formula.constants import BitrixDialect as D

DEFINITIONS_DATETIME = [
    # dateadd
    base.FuncDateadd1.for_dialect(D.BITRIX),
    base.FuncDateadd2Unit.for_dialect(D.BITRIX),
    base.FuncDateadd2Number.for_dialect(D.BITRIX),
]
