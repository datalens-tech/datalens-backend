import dl_formula.definitions.functions_native as base

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


DEFINITIONS_NATIVE = [
    base.DBCallInt.for_dialect(D.CLICKHOUSE),
    base.DBCallFloat.for_dialect(D.CLICKHOUSE),
    base.DBCallString.for_dialect(D.CLICKHOUSE),
    base.DBCallBool.for_dialect(D.CLICKHOUSE),
    base.DBCallArrayInt.for_dialect(D.CLICKHOUSE),
    base.DBCallArrayFloat.for_dialect(D.CLICKHOUSE),
    base.DBCallArrayString.for_dialect(D.CLICKHOUSE),
]
