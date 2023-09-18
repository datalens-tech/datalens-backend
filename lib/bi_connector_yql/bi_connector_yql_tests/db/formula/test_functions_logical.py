from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from bi_connector_yql_tests.db.formula.base import YQLTestBase


class TestLogicalFunctionYQL(YQLTestBase, DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_iif = True
