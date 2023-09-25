from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D
import dl_formula.definitions.functions_markup as base


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
]
