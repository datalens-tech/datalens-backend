import dl_formula.definitions.functions_native as base

from dl_connector_oracle.formula.constants import OracleDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.ORACLE),
    base.DBCallFloat.for_dialect(D.ORACLE),
    base.DBCallString.for_dialect(D.ORACLE),
    base.DBCallBool.for_dialect(D.ORACLE),
    base.DBCallArrayInt.for_dialect(D.ORACLE),
    base.DBCallArrayFloat.for_dialect(D.ORACLE),
    base.DBCallArrayString.for_dialect(D.ORACLE),
    base.DBCallAggInt.for_dialect(D.ORACLE),
    base.DBCallAggFloat.for_dialect(D.ORACLE),
    base.DBCallAggString.for_dialect(D.ORACLE),
]
