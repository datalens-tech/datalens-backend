import dl_formula.definitions.functions_markup as base

from dl_connector_mysql.formula.constants import MySQLDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.MYSQL),
    # __str
    base.FuncInternalStrConst.for_dialect(D.MYSQL),
    base.FuncInternalStr.for_dialect(D.MYSQL),
    # bold
    base.FuncBold.for_dialect(D.MYSQL),
    # italic
    base.FuncItalics.for_dialect(D.MYSQL),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.MYSQL),
    # url
    base.FuncUrl.for_dialect(D.MYSQL),
    # size
    base.FuncSize.for_dialect(D.MYSQL),
    # color
    base.FuncColor.for_dialect(D.MYSQL),
    # br
    base.FuncBr.for_dialect(D.MYSQL),
    # image
    base.FuncImage1.for_dialect(D.MYSQL),
    base.FuncImage2.for_dialect(D.MYSQL),
    base.FuncImage3.for_dialect(D.MYSQL),
    base.FuncImage4.for_dialect(D.MYSQL),
]
