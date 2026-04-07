from dl_formula.connectors.base.connector import FormulaConnector
from dl_sqlalchemy_starrocks.base import BIStarRocksDialect

from dl_connector_starrocks.formula.constants import StarRocksDialect as StarRocksDialectNS
from dl_connector_starrocks.formula.definitions.all import DEFINITIONS
from dl_connector_starrocks.formula.literal import StarRocksLiteralizer
from dl_connector_starrocks.formula.type_constructor import StarRocksTypeConstructor


class StarRocksFormulaConnector(FormulaConnector):
    dialect_ns_cls = StarRocksDialectNS
    dialects = StarRocksDialectNS.STARROCKS
    default_dialect = StarRocksDialectNS.STARROCKS_3_2
    op_definitions = DEFINITIONS
    literalizer_cls = StarRocksLiteralizer
    type_constructor_cls = StarRocksTypeConstructor
    sa_dialect = BIStarRocksDialect()
