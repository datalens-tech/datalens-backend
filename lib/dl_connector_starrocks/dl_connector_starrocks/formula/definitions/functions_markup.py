import dl_formula.definitions.functions_markup as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.STARROCKS),
    # __str
    base.FuncInternalStrConst.for_dialect(D.STARROCKS),
    base.FuncInternalStr.for_dialect(D.STARROCKS),
    # bold
    base.FuncBold.for_dialect(D.STARROCKS),
    # italic
    base.FuncItalics.for_dialect(D.STARROCKS),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.STARROCKS),
    # url
    base.FuncUrl.for_dialect(D.STARROCKS),
    # size
    base.FuncSize.for_dialect(D.STARROCKS),
    # color
    base.FuncColor.for_dialect(D.STARROCKS),
    # br
    base.FuncBr.for_dialect(D.STARROCKS),
    # image
    base.FuncImage1.for_dialect(D.STARROCKS),
    base.FuncImage2.for_dialect(D.STARROCKS),
    base.FuncImage3.for_dialect(D.STARROCKS),
    base.FuncImage4.for_dialect(D.STARROCKS),
    # tooltip
    base.FuncTooltip2.for_dialect(D.STARROCKS),
    base.FuncTooltip3.for_dialect(D.STARROCKS),
    # user_info
    base.FuncUserInfo.for_dialect(D.STARROCKS),
]
