import dl_formula.definitions.functions_native as base

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.POSTGRESQL),
    base.DBCallFloat.for_dialect(D.POSTGRESQL),
    base.DBCallString.for_dialect(D.POSTGRESQL),
    base.DBCallBool.for_dialect(D.POSTGRESQL),
    base.DBCallArrayInt.for_dialect(D.POSTGRESQL),
    base.DBCallArrayFloat.for_dialect(D.POSTGRESQL),
    base.DBCallArrayString.for_dialect(D.POSTGRESQL),
]
