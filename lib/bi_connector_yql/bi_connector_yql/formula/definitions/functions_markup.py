import dl_formula.definitions.functions_markup as base

from bi_connector_yql.formula.constants import YqlDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.YQL),
    # __str
    base.FuncInternalStrConst.for_dialect(D.YQL),
    base.FuncInternalStr.for_dialect(D.YQL),
    # bold
    base.FuncBold.for_dialect(D.YQL),
    # italic
    base.FuncItalics.for_dialect(D.YQL),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.YQL),
    # url
    base.FuncUrl.for_dialect(D.YQL),
]
