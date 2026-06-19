from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class DateTimeFunctionPostgreSQLTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetimetz = True


class TestDateTimeFunctionPostgreSQL9p3(PostgreSQL9p3TestBase, DateTimeFunctionPostgreSQLTestSuite):
    pass


class TestDateTimeFunctionPostgreSQL9p4(PostgreSQL9p4TestBase, DateTimeFunctionPostgreSQLTestSuite):
    pass
