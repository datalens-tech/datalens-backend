from dl_formula.core.dialect import (
    DialectName,
    simple_combo,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect


DIALECT_NAME_FILE = DialectName.declare("FILE")


class FileS3Dialect(ClickHouseDialect):
    FILE = simple_combo(name=DIALECT_NAME_FILE)
