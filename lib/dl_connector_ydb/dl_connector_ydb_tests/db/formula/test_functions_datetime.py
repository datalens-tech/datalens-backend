from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestDateTimeFunctionYQL(YQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_datepart_2_non_const = False
