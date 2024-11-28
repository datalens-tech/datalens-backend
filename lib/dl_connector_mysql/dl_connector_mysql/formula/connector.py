from sqlalchemy.dialects.mysql.base import MySQLDialect as SAMySQLDialect

from dl_formula.connectors.base.connector import FormulaConnector

from dl_connector_mysql.formula.constants import MySQLDialect as MySQLDialectNS
from dl_connector_mysql.formula.definitions.all import DEFINITIONS
from dl_connector_mysql.formula.literal import MySQLLiteralizer
from dl_connector_mysql.formula.type_constructor import MySQLTypeConstructor


class MySQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = MySQLDialectNS
    dialects = MySQLDialectNS.MYSQL
    default_dialect = MySQLDialectNS.MYSQL_8_0_40
    op_definitions = DEFINITIONS
    literalizer_cls = MySQLLiteralizer
    type_constructor_cls = MySQLTypeConstructor
    sa_dialect = SAMySQLDialect()
