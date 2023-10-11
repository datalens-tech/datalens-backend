from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_ORACLE = DialectName.declare("ORACLE")


class OracleDialect(DialectNamespace):
    ORACLE_12_0 = simple_combo(name=DIALECT_NAME_ORACLE, version=(12, 0))
    ORACLE = ORACLE_12_0
