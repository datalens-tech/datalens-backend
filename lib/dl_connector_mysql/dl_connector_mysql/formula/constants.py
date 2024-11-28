from dl_formula.core.dialect import (
    DialectName,
    DialectNamespace,
    simple_combo,
)


DIALECT_NAME_MYSQL = DialectName.declare("MYSQL")


class MySQLDialect(DialectNamespace):
    MYSQL_5_7 = simple_combo(name=DIALECT_NAME_MYSQL, version=(5, 7))
    MYSQL_8_0_40 = simple_combo(name=DIALECT_NAME_MYSQL, version=(8, 0, 40))
    MYSQL = MYSQL_5_7 | MYSQL_8_0_40
