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
]
