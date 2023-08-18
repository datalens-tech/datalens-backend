from bi_formula.core.dialect import DialectNamespace, DialectName, simple_combo


class OracleDialect(DialectNamespace):
    ORACLE_12_0 = simple_combo(name=DialectName.ORACLE, version=(12, 0))
    ORACLE = ORACLE_12_0
