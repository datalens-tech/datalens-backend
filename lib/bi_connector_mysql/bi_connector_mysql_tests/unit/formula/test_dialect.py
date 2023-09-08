from bi_formula_testing.testcases.dialect import DefaultDialectFormulaConnectorTestSuite

from bi_connector_mysql.formula.constants import DIALECT_NAME_MYSQL
from bi_connector_mysql.formula.constants import MySQLDialect as D


class DialectMySQLTestSuite(DefaultDialectFormulaConnectorTestSuite):
    dialect_name = DIALECT_NAME_MYSQL
    default_dialect = D.MYSQL_8_0_12
    dialect_matches = (
        ('5.6', D.MYSQL_5_6),
        ('5.7.23-0ubuntu0.18.04.1', D.MYSQL_5_7),
        ('8.2.3', D.MYSQL_8_0_12),
    )
