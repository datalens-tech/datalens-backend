from dl_connector_bundle_chs3.chs3_yadocs.formula.constants import YaDocsFileS3Dialect as YaDocsDialectNS
from dl_connector_bundle_chs3.chs3_yadocs.formula.definitions.all import DEFINITIONS
from dl_connector_clickhouse.formula.connector import ClickHouseFormulaConnector


class YaDocsFileS3FormulaConnector(ClickHouseFormulaConnector):
    dialect_ns_cls = YaDocsDialectNS
    dialects = YaDocsDialectNS.YADOCS
    default_dialect = YaDocsDialectNS.YADOCS
    op_definitions = DEFINITIONS
