from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class MssqlDialect(DialectNamespace):
    MSSQLSRV_14_0 = simple_combo(name=DialectName.MSSQLSRV, version=(14, 0))
    MSSQLSRV = MSSQLSRV_14_0
