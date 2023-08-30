from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect as SAClickHouseDialect

from bi_formula.connectors.base.connector import FormulaConnector

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as ClickHouseDialectNS
from bi_connector_clickhouse.formula.definitions.all import DEFINITIONS
from bi_connector_clickhouse.formula.literal import ClickHouseLiteralizer
from bi_connector_clickhouse.formula.type_constructor import ClickHouseTypeConstructor


class ClickHouseFormulaConnector(FormulaConnector):
    dialect_ns_cls = ClickHouseDialectNS
    dialects = ClickHouseDialectNS.CLICKHOUSE
    default_dialect = ClickHouseDialectNS.CLICKHOUSE_21_8
    op_definitions = DEFINITIONS
    literalizer_cls = ClickHouseLiteralizer
    type_constructor_cls = ClickHouseTypeConstructor
    sa_dialect = SAClickHouseDialect()
