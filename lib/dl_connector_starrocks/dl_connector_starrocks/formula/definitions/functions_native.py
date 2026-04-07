import dl_formula.definitions.functions_native as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.STARROCKS),
    base.DBCallFloat.for_dialect(D.STARROCKS),
    base.DBCallString.for_dialect(D.STARROCKS),
    base.DBCallBool.for_dialect(D.STARROCKS),
    base.DBCallArrayInt.for_dialect(D.STARROCKS),
    base.DBCallArrayFloat.for_dialect(D.STARROCKS),
    base.DBCallArrayString.for_dialect(D.STARROCKS),
    base.DBCallAggInt.for_dialect(D.STARROCKS),
    base.DBCallAggFloat.for_dialect(D.STARROCKS),
    base.DBCallAggString.for_dialect(D.STARROCKS),
]
