from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class PostgreSQLDialect(DialectNamespace):
    COMPENG = simple_combo(name=DialectName.COMPENG)
    POSTGRESQL_9_3 = simple_combo(name=DialectName.POSTGRESQL, version=(9, 3))
    POSTGRESQL_9_4 = simple_combo(name=DialectName.POSTGRESQL, version=(9, 4))
    NON_COMPENG_POSTGRESQL = POSTGRESQL_9_3 | POSTGRESQL_9_4
    POSTGRESQL = COMPENG | NON_COMPENG_POSTGRESQL
