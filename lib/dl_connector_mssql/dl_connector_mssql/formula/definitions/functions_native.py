import dl_formula.definitions.functions_native as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.MSSQLSRV),
    base.DBCallFloat.for_dialect(D.MSSQLSRV),
    base.DBCallString.for_dialect(D.MSSQLSRV),
    base.DBCallBool.for_dialect(D.MSSQLSRV),
    base.DBCallArrayInt.for_dialect(D.MSSQLSRV),
    base.DBCallArrayFloat.for_dialect(D.MSSQLSRV),
    base.DBCallArrayString.for_dialect(D.MSSQLSRV),
    base.DBCallAggInt.for_dialect(D.MSSQLSRV),
    base.DBCallAggFloat.for_dialect(D.MSSQLSRV),
    base.DBCallAggString.for_dialect(D.MSSQLSRV),
]
