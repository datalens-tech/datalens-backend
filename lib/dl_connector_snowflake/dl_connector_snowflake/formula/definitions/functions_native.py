import dl_formula.definitions.functions_native as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.SNOWFLAKE),
    base.DBCallFloat.for_dialect(D.SNOWFLAKE),
    base.DBCallString.for_dialect(D.SNOWFLAKE),
    base.DBCallBool.for_dialect(D.SNOWFLAKE),
    base.DBCallArrayInt.for_dialect(D.SNOWFLAKE),
    base.DBCallArrayFloat.for_dialect(D.SNOWFLAKE),
    base.DBCallArrayString.for_dialect(D.SNOWFLAKE),
    base.DBCallAggInt.for_dialect(D.SNOWFLAKE),
    base.DBCallAggFloat.for_dialect(D.SNOWFLAKE),
    base.DBCallAggString.for_dialect(D.SNOWFLAKE),
]
