from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_POSTGRESQL = DialectName.declare("POSTGRESQL")
DIALECT_NAME_COMPENG = DialectName.declare("COMPENG")  # computation engine (PostgreSQL-based)


class PostgreSQLDialect(DialectNamespace):
    COMPENG = simple_combo(name=DIALECT_NAME_COMPENG)
    POSTGRESQL_9_3 = simple_combo(name=DIALECT_NAME_POSTGRESQL, version=(9, 3))
    POSTGRESQL_9_4 = simple_combo(name=DIALECT_NAME_POSTGRESQL, version=(9, 4))
    NON_COMPENG_POSTGRESQL = POSTGRESQL_9_3 | POSTGRESQL_9_4
    POSTGRESQL = COMPENG | NON_COMPENG_POSTGRESQL
