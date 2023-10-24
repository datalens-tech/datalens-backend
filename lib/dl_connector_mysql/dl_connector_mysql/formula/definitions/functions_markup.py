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
]
