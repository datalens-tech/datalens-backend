from ydb.sqlalchemy import YqlDialect as SAYqlDialect

from dl_formula.connectors.base.connector import FormulaConnector

from dl_connector_ydb.formula.constants import YqlDialect as YqlDialectNS
from dl_connector_ydb.formula.definitions.all import DEFINITIONS


class YQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = YqlDialectNS
    dialects = YqlDialectNS.YQL
    op_definitions = DEFINITIONS
    sa_dialect = SAYqlDialect()
