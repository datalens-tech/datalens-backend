import dl_formula.definitions.functions_markup as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.BIGQUERY),
    # __str
    base.FuncInternalStrConst.for_dialect(D.BIGQUERY),
    base.FuncInternalStr.for_dialect(D.BIGQUERY),
    # bold
    base.FuncBold.for_dialect(D.BIGQUERY),
    # italic
    base.FuncItalics.for_dialect(D.BIGQUERY),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.BIGQUERY),
    # url
    base.FuncUrl.for_dialect(D.BIGQUERY),
    # size
    base.FuncSize.for_dialect(D.BIGQUERY),
    # color
    base.FuncColor.for_dialect(D.BIGQUERY),
    # br
    base.FuncBr.for_dialect(D.BIGQUERY),
    # image
    base.FuncImage1.for_dialect(D.BIGQUERY),
    base.FuncImage2.for_dialect(D.BIGQUERY),
    base.FuncImage3.for_dialect(D.BIGQUERY),
    base.FuncImage4.for_dialect(D.BIGQUERY),
    # tooltip
    base.FuncTooltip2.for_dialect(D.BIGQUERY),
    base.FuncTooltip3.for_dialect(D.BIGQUERY),
    # user_info
    base.FuncUserInfo.for_dialect(D.BIGQUERY),
]
