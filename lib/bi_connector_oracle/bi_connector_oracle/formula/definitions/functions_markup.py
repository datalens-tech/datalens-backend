import dl_formula.definitions.functions_markup as base

from bi_connector_oracle.formula.constants import OracleDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.ORACLE),

    # __str
    base.FuncInternalStrConst.for_dialect(D.ORACLE),
    base.FuncInternalStr.for_dialect(D.ORACLE),

    # bold
    base.FuncBold.for_dialect(D.ORACLE),

    # italic
    base.FuncItalics.for_dialect(D.ORACLE),

    # markup
    base.ConcatMultiMarkup.for_dialect(D.ORACLE),

    # url
    base.FuncUrl.for_dialect(D.ORACLE),
]
