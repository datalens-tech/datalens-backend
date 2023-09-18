from ydb.sqlalchemy import YqlDialect as SAYqlDialect

from dl_formula.connectors.base.connector import FormulaConnector

from bi_connector_yql.formula.constants import YqlDialect as YqlDialectNS
from bi_connector_yql.formula.definitions.all import DEFINITIONS


class YQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = YqlDialectNS
    dialects = YqlDialectNS.YQL
    op_definitions = DEFINITIONS
    sa_dialect = SAYqlDialect()
