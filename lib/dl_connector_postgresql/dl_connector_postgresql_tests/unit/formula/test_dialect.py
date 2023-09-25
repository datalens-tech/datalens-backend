from dl_connector_postgresql.formula.constants import DIALECT_NAME_POSTGRESQL
from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_formula_testing.testcases.dialect import DefaultDialectFormulaConnectorTestSuite


class DialectPostgreSQLTestSuite(DefaultDialectFormulaConnectorTestSuite):
    dialect_name = DIALECT_NAME_POSTGRESQL
    default_dialect = D.POSTGRESQL_9_4
    dialect_matches = (
        ("9.3.4", D.POSTGRESQL_9_3),
        ("9.6.1", D.POSTGRESQL_9_4),
    )
