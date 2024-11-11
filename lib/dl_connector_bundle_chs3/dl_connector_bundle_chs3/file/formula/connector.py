from dl_connector_bundle_chs3.file.formula.constants import FileS3Dialect as FileDialectNS
from dl_connector_bundle_chs3.file.formula.definitions.all import DEFINITIONS
from dl_connector_clickhouse.formula.connector import ClickHouseFormulaConnector


class FileS3FormulaConnector(ClickHouseFormulaConnector):
    dialect_ns_cls = FileDialectNS
    dialects = FileDialectNS.FILE
    default_dialect = FileDialectNS.FILE
    op_definitions = DEFINITIONS
