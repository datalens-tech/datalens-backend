from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_TRINO = DialectName.declare("TRINO")


class TrinoDialect(DialectNamespace):
    TRINO_468 = simple_combo(name=DIALECT_NAME_TRINO, version=(468,))
    TRINO = TRINO_468
