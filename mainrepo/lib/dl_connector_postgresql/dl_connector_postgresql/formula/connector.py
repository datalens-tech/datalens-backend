from sqlalchemy.dialects.postgresql.base import PGDialect

from dl_formula.connectors.base.connector import FormulaConnector

from dl_connector_postgresql.formula.constants import PostgreSQLDialect
from dl_connector_postgresql.formula.definitions.all import DEFINITIONS
from dl_connector_postgresql.formula.literal import GenericPostgreSQLLiteralizer
from dl_connector_postgresql.formula.type_constructor import PostgreSQLTypeConstructor


class PostgreSQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = PostgreSQLDialect
    dialects = PostgreSQLDialect.POSTGRESQL
    default_dialect = PostgreSQLDialect.POSTGRESQL_9_4
    op_definitions = DEFINITIONS
    literalizer_cls = GenericPostgreSQLLiteralizer
    type_constructor_cls = PostgreSQLTypeConstructor
    sa_dialect = PGDialect()
