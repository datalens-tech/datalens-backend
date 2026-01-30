from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_STARROCKS = DialectName.declare("STARROCKS")


class StarRocksDialect(DialectNamespace):
    STARROCKS_3_0 = simple_combo(name=DIALECT_NAME_STARROCKS, version=(3, 0))
    STARROCKS = STARROCKS_3_0
