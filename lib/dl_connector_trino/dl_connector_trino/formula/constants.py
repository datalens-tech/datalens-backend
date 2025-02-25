from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_TRINO = DialectName.declare("TRINO")


class TrinoDialect(DialectNamespace):
    TRINO_471 = simple_combo(name=DIALECT_NAME_TRINO, version=(471,))
    TRINO = TRINO_471
