import dl_formula.definitions.functions_string as base

from dl_connector_bitrix_gds.formula.constants import BitrixDialect as D


DEFINITIONS_STRING = [
    # concat
    base.ConcatMultiStrConst.for_dialect(D.BITRIX),
]
