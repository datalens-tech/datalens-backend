import dl_formula.definitions.functions_markup as base

from dl_connector_ydb.formula.constants import YqlDialect as D


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
    # size
    base.FuncSize.for_dialect(D.YQL),
    # color
    base.FuncColor.for_dialect(D.YQL),
    # br
    base.FuncBr.for_dialect(D.YQL),
    # image
    base.FuncImage1.for_dialect(D.YQL),
    base.FuncImage2.for_dialect(D.YQL),
    base.FuncImage3.for_dialect(D.YQL),
    base.FuncImage4.for_dialect(D.YQL),
    # tooltip
    base.FuncTooltip2.for_dialect(D.YQL),
    base.FuncTooltip3.for_dialect(D.YQL),
    # user_info
    base.FuncUserInfo.for_dialect(D.YQL),
]
