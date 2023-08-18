from sqlalchemy.dialects.postgresql.base import PGDialect

from bi_formula.connectors.base.connector import FormulaConnector

from bi_connector_postgresql.formula.constants import PostgreSQLDialect
from bi_connector_postgresql.formula.definitions.all import DEFINITIONS
from bi_connector_postgresql.formula.literal import GenericPostgreSQLLiteralizer
from bi_connector_postgresql.formula.type_constructor import PostgreSQLTypeConstructor


class PostgreSQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = PostgreSQLDialect
    dialects = PostgreSQLDialect.POSTGRESQL
    default_dialect = PostgreSQLDialect.POSTGRESQL_9_4
    op_definitions = DEFINITIONS
    literalizer_cls = GenericPostgreSQLLiteralizer
    type_constructor_cls = PostgreSQLTypeConstructor
    sa_dialect = PGDialect()
