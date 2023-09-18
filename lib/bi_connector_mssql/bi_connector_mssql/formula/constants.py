from dl_formula.core.dialect import DialectNamespace, DialectName, simple_combo


DIALECT_NAME_MSSQLSRV = DialectName.declare('MSSQLSRV')


class MssqlDialect(DialectNamespace):
    MSSQLSRV_14_0 = simple_combo(name=DIALECT_NAME_MSSQLSRV, version=(14, 0))
    MSSQLSRV = MSSQLSRV_14_0
