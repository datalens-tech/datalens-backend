from sqlalchemy.dialects.mysql.base import MySQLDialect as SAMySQLDialect

from dl_formula.connectors.base.connector import FormulaConnector

from dl_connector_starrocks.formula.constants import StarRocksDialect as StarRocksDialectNS


class StarRocksFormulaConnector(FormulaConnector):
    dialect_ns_cls = StarRocksDialectNS
    dialects = StarRocksDialectNS.STARROCKS
    default_dialect = StarRocksDialectNS.STARROCKS_3_0
    op_definitions = ()
    sa_dialect = SAMySQLDialect()
