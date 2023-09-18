from sqlalchemy.dialects.mssql.base import MSDialect

from dl_formula.connectors.base.connector import FormulaConnector

from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_mssql.formula.context_processor import MSSQLContextPostprocessor
from bi_connector_mssql.formula.definitions.all import DEFINITIONS
from bi_connector_mssql.formula.literal import MSSQLLiteralizer
from bi_connector_mssql.formula.type_constructor import MSSQLTypeConstructor


class MSSQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = MssqlDialect
    dialects = MssqlDialect.MSSQLSRV
    default_dialect = MssqlDialect.MSSQLSRV_14_0
    op_definitions = DEFINITIONS
    literalizer_cls = MSSQLLiteralizer
    context_processor_cls = MSSQLContextPostprocessor
    type_constructor_cls = MSSQLTypeConstructor
    sa_dialect = MSDialect()
