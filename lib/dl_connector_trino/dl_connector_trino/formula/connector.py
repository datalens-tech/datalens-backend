from trino.sqlalchemy.dialect import TrinoDialect as SATrinoDialect

from dl_formula.connectors.base.connector import FormulaConnector

from dl_connector_trino.formula.constants import TrinoDialect as TrinoDialectNS
from dl_connector_trino.formula.definitions.all import DEFINITIONS
from dl_connector_trino.formula.literal import TrinoLiteralizer
from dl_connector_trino.formula.type_constructor import TrinoTypeConstructor


class TrinoFormulaConnector(FormulaConnector):
    dialect_ns_cls = TrinoDialectNS
    dialects = TrinoDialectNS.TRINO
    default_dialect = TrinoDialectNS.TRINO
    op_definitions = DEFINITIONS
    literalizer_cls = TrinoLiteralizer
    type_constructor_cls = TrinoTypeConstructor
    sa_dialect = SATrinoDialect()
