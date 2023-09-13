import bi_formula.definitions.operators_binary as base

from bi_connector_bitrix_gds.formula.constants import BitrixDialect as D


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.BITRIX),

    # <
    base.BinaryLessThan.for_dialect(D.BITRIX),

    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.BITRIX),

    # ==
    base.BinaryEqual.for_dialect(D.BITRIX),

    # >
    base.BinaryGreaterThan.for_dialect(D.BITRIX),

    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.BITRIX),

    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.BITRIX),

    # _==
    base.BinaryEqualInternal.for_dialect(D.BITRIX),

    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.BITRIX),
]
