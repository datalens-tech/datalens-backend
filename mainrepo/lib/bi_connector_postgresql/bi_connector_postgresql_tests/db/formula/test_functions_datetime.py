from bi_formula_testing.testcases.functions_datetime import (
    DefaultDateTimeFunctionFormulaConnectorTestSuite,
)

from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase, PostgreSQL_9_4TestBase,
)


class DateTimeFunctionPostgreSQLTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetimetz = True


class TestDateTimeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DateTimeFunctionPostgreSQLTestSuite):
    pass


class TestDateTimeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DateTimeFunctionPostgreSQLTestSuite):
    pass
