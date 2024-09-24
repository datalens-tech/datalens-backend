from sqlalchemy.dialects.oracle.base import OracleDialect as SAOracleDialect

from dl_formula.connectors.base.connector import FormulaConnector
from dl_formula.mutation.optimization import OptimizeUnaryBoolFunctions

from dl_connector_oracle.formula.constants import OracleDialect as OracleDialectNS
from dl_connector_oracle.formula.context_processor import OracleContextPostprocessor
from dl_connector_oracle.formula.definitions.all import DEFINITIONS
from dl_connector_oracle.formula.literal import OracleLiteralizer
from dl_connector_oracle.formula.type_constructor import OracleTypeConstructor


class OracleFormulaConnector(FormulaConnector):
    dialect_ns_cls = OracleDialectNS
    dialects = OracleDialectNS.ORACLE
    default_dialect = OracleDialectNS.ORACLE
    op_definitions = DEFINITIONS
    literalizer_cls = OracleLiteralizer
    context_processor_cls = OracleContextPostprocessor
    type_constructor_cls = OracleTypeConstructor
    sa_dialect = SAOracleDialect()

    @classmethod
    def registration_hook(cls) -> None:
        OptimizeUnaryBoolFunctions.register_dialect(
            "isnull", cls.dialects, f=lambda x: x is None or (isinstance(x, str) and x == "")
        )
