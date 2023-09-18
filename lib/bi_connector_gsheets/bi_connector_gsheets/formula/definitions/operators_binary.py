import dl_formula.definitions.operators_binary as base

from bi_connector_gsheets.formula.constants import GSheetsDialect as D

DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.GSHEETS),
    # ==
    base.BinaryEqual.for_dialect(D.GSHEETS),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.GSHEETS),
    # _==
    base.BinaryEqualInternal.for_dialect(D.GSHEETS),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.GSHEETS),
]
