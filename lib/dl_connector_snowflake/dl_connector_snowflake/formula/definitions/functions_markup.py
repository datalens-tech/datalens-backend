import dl_formula.definitions.functions_markup as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.SNOWFLAKE),
    # __str
    base.FuncInternalStrConst.for_dialect(D.SNOWFLAKE),
    base.FuncInternalStr.for_dialect(D.SNOWFLAKE),
    # bold
    base.FuncBold.for_dialect(D.SNOWFLAKE),
    # italic
    base.FuncItalics.for_dialect(D.SNOWFLAKE),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.SNOWFLAKE),
    # url
    base.FuncUrl.for_dialect(D.SNOWFLAKE),
    # size
    base.FuncSize.for_dialect(D.SNOWFLAKE),
    # color
    base.FuncColor.for_dialect(D.SNOWFLAKE),
    # br
    base.FuncBr.for_dialect(D.SNOWFLAKE),
    # image
    base.FuncImage1.for_dialect(D.SNOWFLAKE),
    base.FuncImage2.for_dialect(D.SNOWFLAKE),
    base.FuncImage3.for_dialect(D.SNOWFLAKE),
    base.FuncImage4.for_dialect(D.SNOWFLAKE),
    # tooltip
    base.FuncTooltip2.for_dialect(D.SNOWFLAKE),
    base.FuncTooltip3.for_dialect(D.SNOWFLAKE),
    # user_info
    base.FuncUserInfo.for_dialect(D.SNOWFLAKE),
]
