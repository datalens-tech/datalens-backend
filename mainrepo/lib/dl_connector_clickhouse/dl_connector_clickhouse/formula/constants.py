from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_CLICKHOUSE = DialectName.declare("CLICKHOUSE")


class ClickHouseDialect(DialectNamespace):
    CLICKHOUSE_21_8 = simple_combo(name=DIALECT_NAME_CLICKHOUSE, version=(21, 8))
    CLICKHOUSE_22_10 = simple_combo(name=DIALECT_NAME_CLICKHOUSE, version=(22, 10))
    CLICKHOUSE = CLICKHOUSE_21_8 | CLICKHOUSE_22_10
