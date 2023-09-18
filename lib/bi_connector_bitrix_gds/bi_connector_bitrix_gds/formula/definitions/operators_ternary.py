import dl_formula.definitions.operators_ternary as base

from bi_connector_bitrix_gds.formula.constants import BitrixDialect as D


DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.BITRIX),
]
