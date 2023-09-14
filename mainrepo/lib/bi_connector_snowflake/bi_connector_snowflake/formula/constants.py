from bi_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)

DIALECT_NAME_SNOWFLAKE = DialectName.declare("SNOWFLAKE")


class SnowFlakeDialect(DialectNamespace):
    SNOWFLAKE = simple_combo(name=DIALECT_NAME_SNOWFLAKE)
