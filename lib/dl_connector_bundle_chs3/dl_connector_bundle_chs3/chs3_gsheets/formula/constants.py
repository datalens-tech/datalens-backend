from dl_formula.core.dialect import (
    DialectName,
    simple_combo,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect


DIALECT_NAME_GSHEETS_V2 = DialectName.declare("GSHEETS_V2")


class GSheetsFileS3Dialect(ClickHouseDialect):
    GSHEETS_V2 = simple_combo(name=DIALECT_NAME_GSHEETS_V2)
