import dl_formula.definitions.functions_markup as base

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.POSTGRESQL),
    # __str
    base.FuncInternalStrConst.for_dialect(D.POSTGRESQL),
    base.FuncInternalStr.for_dialect(D.POSTGRESQL),
    # bold
    base.FuncBold.for_dialect(D.POSTGRESQL),
    # italic
    base.FuncItalics.for_dialect(D.POSTGRESQL),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.POSTGRESQL),
    # url
    base.FuncUrl.for_dialect(D.POSTGRESQL),
    # size
    base.FuncSize.for_dialect(D.POSTGRESQL),
    # color
    base.FuncColor.for_dialect(D.POSTGRESQL),
    # br
    base.FuncBr.for_dialect(D.POSTGRESQL),
]
