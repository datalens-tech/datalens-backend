import dl_formula.definitions.functions_markup as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.MSSQLSRV),
    # __str
    base.FuncInternalStrConst.for_dialect(D.MSSQLSRV),
    base.FuncInternalStr.for_dialect(D.MSSQLSRV),
    # bold
    base.FuncBold.for_dialect(D.MSSQLSRV),
    # italic
    base.FuncItalics.for_dialect(D.MSSQLSRV),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.MSSQLSRV),
    # url
    base.FuncUrl.for_dialect(D.MSSQLSRV),
    # size
    base.FuncSize.for_dialect(D.MSSQLSRV),
    # color
    base.FuncColor.for_dialect(D.MSSQLSRV),
    # br
    base.FuncBr.for_dialect(D.MSSQLSRV),
]
