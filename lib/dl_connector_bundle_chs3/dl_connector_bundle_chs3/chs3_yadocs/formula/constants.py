from dl_formula.core.dialect import (
    DialectName,
    simple_combo,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect


DIALECT_NAME_YADOCS = DialectName.declare("YADOCS")


class YaDocsFileS3Dialect(ClickHouseDialect):
    YADOCS = simple_combo(name=DIALECT_NAME_YADOCS)
