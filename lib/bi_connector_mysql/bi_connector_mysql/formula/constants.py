from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_MYSQL = DialectName.declare('MYSQL')


class MySQLDialect(DialectNamespace):
    MYSQL_5_6 = simple_combo(name=DIALECT_NAME_MYSQL, version=(5, 6))
    MYSQL_5_7 = simple_combo(name=DIALECT_NAME_MYSQL, version=(5, 7))
    MYSQL_8_0_12 = simple_combo(name=DIALECT_NAME_MYSQL, version=(8, 0, 12))
    MYSQL = MYSQL_5_6 | MYSQL_5_7 | MYSQL_8_0_12
