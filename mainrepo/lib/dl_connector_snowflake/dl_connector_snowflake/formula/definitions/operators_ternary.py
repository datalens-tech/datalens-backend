from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D
import dl_formula.definitions.operators_ternary as base

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.SNOWFLAKE),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.SNOWFLAKE),
]
