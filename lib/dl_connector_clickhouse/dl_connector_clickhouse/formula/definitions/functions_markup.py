import dl_formula.definitions.functions_markup as base

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


DEFINITIONS_MARKUP = [
    # +
    base.BinaryPlusMarkup.for_dialect(D.CLICKHOUSE),
    # __str
    base.FuncInternalStrConst.for_dialect(D.CLICKHOUSE),
    base.FuncInternalStr.for_dialect(D.CLICKHOUSE),
    # bold
    base.FuncBold.for_dialect(D.CLICKHOUSE),
    # italic
    base.FuncItalics.for_dialect(D.CLICKHOUSE),
    # markup
    base.ConcatMultiMarkup.for_dialect(D.CLICKHOUSE),
    # url
    base.FuncUrl.for_dialect(D.CLICKHOUSE),
    # size
    base.FuncSize.for_dialect(D.CLICKHOUSE),
    # color
    base.FuncColor.for_dialect(D.CLICKHOUSE),
    # br
    base.FuncBr.for_dialect(D.CLICKHOUSE),
    # image
    base.FuncImage1.for_dialect(D.CLICKHOUSE),
    base.FuncImage2.for_dialect(D.CLICKHOUSE),
    base.FuncImage3.for_dialect(D.CLICKHOUSE),
    base.FuncImage4.for_dialect(D.CLICKHOUSE),
]
