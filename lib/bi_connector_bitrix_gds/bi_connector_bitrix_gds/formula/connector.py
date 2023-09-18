from dl_formula.connectors.base.connector import FormulaConnector

from bi_sqlalchemy_bitrix.base import BitrixDialect as SABitrixDialect
from bi_connector_bitrix_gds.formula.constants import BitrixDialect as BitrixDialectNS
from bi_connector_bitrix_gds.formula.definitions.all import DEFINITIONS


class BitrixGDSFormulaConnector(FormulaConnector):
    dialect_ns_cls = BitrixDialectNS
    dialects = BitrixDialectNS.BITRIX
    default_dialect = BitrixDialectNS.BITRIX
    op_definitions = DEFINITIONS
    sa_dialect = SABitrixDialect()
