from dl_formula.connectors.base.column import UnprefixedColumnRenderer
from dl_formula.connectors.base.connector import FormulaConnector

from bi_sqlalchemy_gsheets import GSheetsDialect as SAGSheetsDialect
from bi_connector_gsheets.formula.constants import GSheetsDialect as GSheetsDialectNS
from bi_connector_gsheets.formula.definitions.all import DEFINITIONS


class GSheetsFormulaConnector(FormulaConnector):
    dialect_ns_cls = GSheetsDialectNS
    dialects = GSheetsDialectNS.GSHEETS
    default_dialect = GSheetsDialectNS.GSHEETS
    op_definitions = DEFINITIONS
    column_renderer_cls = UnprefixedColumnRenderer
    sa_dialect = SAGSheetsDialect()
