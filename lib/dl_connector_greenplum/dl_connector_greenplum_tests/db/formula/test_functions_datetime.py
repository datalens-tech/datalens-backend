from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


class DateTimeFunctionGreenplumTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetimetz = True


class TestDateTimeFunctionGreenplum(GreenplumTestBase, DateTimeFunctionGreenplumTestSuite):
    pass
