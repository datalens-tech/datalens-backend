import bi_formula.definitions.operators_ternary as base

from bi_connector_snowflake.formula.constants import SnowFlakeDialect as D

DEFINITIONS_TERNARY = [
    # between
    base.TernaryBetween.for_dialect(D.SNOWFLAKE),
    # notbetween
    base.TernaryNotBetween.for_dialect(D.SNOWFLAKE),
]
