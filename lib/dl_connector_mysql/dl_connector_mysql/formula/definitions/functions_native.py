import dl_formula.definitions.functions_native as base

from dl_connector_mysql.formula.constants import MySQLDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.MYSQL),
    base.DBCallFloat.for_dialect(D.MYSQL),
    base.DBCallString.for_dialect(D.MYSQL),
    base.DBCallBool.for_dialect(D.MYSQL),
    base.DBCallArrayInt.for_dialect(D.MYSQL),
    base.DBCallArrayFloat.for_dialect(D.MYSQL),
    base.DBCallArrayString.for_dialect(D.MYSQL),
    base.DBCallAggInt.for_dialect(D.MYSQL),
    base.DBCallAggFloat.for_dialect(D.MYSQL),
    base.DBCallAggString.for_dialect(D.MYSQL),
]
