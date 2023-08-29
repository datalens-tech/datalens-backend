from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_SNOWFLAKE = DialectName.declare('SNOWFLAKE')


class SnowFlakeDialect(DialectNamespace):
    SNOWFLAKE = simple_combo(name=DIALECT_NAME_SNOWFLAKE)
