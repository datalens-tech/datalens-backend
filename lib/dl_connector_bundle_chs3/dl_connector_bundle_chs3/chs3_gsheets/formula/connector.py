from dl_connector_bundle_chs3.chs3_gsheets.formula.constants import GSheetsFileS3Dialect as GSheetsDialectNS
from dl_connector_bundle_chs3.chs3_gsheets.formula.definitions.all import DEFINITIONS
from dl_connector_clickhouse.formula.connector import ClickHouseFormulaConnector


class GSheetsFileS3FormulaConnector(ClickHouseFormulaConnector):
    dialect_ns_cls = GSheetsDialectNS
    dialects = GSheetsDialectNS.GSHEETS_V2
    default_dialect = GSheetsDialectNS.GSHEETS_V2
    op_definitions = DEFINITIONS
