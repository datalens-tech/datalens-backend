from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class SnowFlakeDialect(DialectNamespace):
    SNOWFLAKE = simple_combo(name=DialectName.SNOWFLAKE)
