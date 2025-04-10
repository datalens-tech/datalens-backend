import dl_formula.definitions.functions_markup as base

from dl_connector_trino.formula.constants import TrinoDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.TRINO),
    # __str
    base.FuncInternalStrConst.for_dialect(D.TRINO),
    base.FuncInternalStr.for_dialect(D.TRINO),
    # bold
    base.FuncBold.for_dialect(D.TRINO),
    # italic
    base.FuncItalics.for_dialect(D.TRINO),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.TRINO),
    # url
    base.FuncUrl.for_dialect(D.TRINO),
    # size
    base.FuncSize.for_dialect(D.TRINO),
    # color
    base.FuncColor.for_dialect(D.TRINO),
    # br
    base.FuncBr.for_dialect(D.TRINO),
    # image
    base.FuncImage1.for_dialect(D.TRINO),
    base.FuncImage2.for_dialect(D.TRINO),
    base.FuncImage3.for_dialect(D.TRINO),
    base.FuncImage4.for_dialect(D.TRINO),
    # tooltip
    base.FuncTooltip2.for_dialect(D.TRINO),
    base.FuncTooltip3.for_dialect(D.TRINO),
]
