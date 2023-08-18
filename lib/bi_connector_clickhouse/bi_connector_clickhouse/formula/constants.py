from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class ClickHouseDialect(DialectNamespace):
    CLICKHOUSE_21_8 = simple_combo(name=DialectName.CLICKHOUSE, version=(21, 8))
    CLICKHOUSE_22_10 = simple_combo(name=DialectName.CLICKHOUSE, version=(22, 10))
    CLICKHOUSE = CLICKHOUSE_21_8 | CLICKHOUSE_22_10
