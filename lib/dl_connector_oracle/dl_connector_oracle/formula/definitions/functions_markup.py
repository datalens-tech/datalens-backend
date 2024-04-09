import dl_formula.definitions.functions_markup as base

from dl_connector_oracle.formula.constants import OracleDialect as D


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
    # size
    base.FuncSize.for_dialect(D.ORACLE),
    # color
    base.FuncColor.for_dialect(D.ORACLE),
    # br
    base.FuncBr.for_dialect(D.ORACLE),
    # image
    base.FuncImage1.for_dialect(D.ORACLE),
    base.FuncImage2.for_dialect(D.ORACLE),
    base.FuncImage3.for_dialect(D.ORACLE),
    base.FuncImage4.for_dialect(D.ORACLE),
]
