from snowflake.sqlalchemy.snowdialect import SnowflakeDialect as SASnowflakeDialect

from bi_formula.connectors.base.connector import FormulaConnector

from bi_connector_snowflake.formula.constants import SnowFlakeDialect as SnowFlakeDialectNS
from bi_connector_snowflake.formula.definitions.all import DEFINITIONS
from bi_connector_snowflake.formula.literal import SnowFlakeLiteralizer


class SnowFlakeFormulaConnector(FormulaConnector):
    dialect_ns_cls = SnowFlakeDialectNS
    dialects = SnowFlakeDialectNS.SNOWFLAKE
    default_dialect = SnowFlakeDialectNS.SNOWFLAKE
    op_definitions = DEFINITIONS
    literalizer_cls = SnowFlakeLiteralizer
    sa_dialect = SASnowflakeDialect()
