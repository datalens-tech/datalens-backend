from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from bi_connector_mssql_tests.db.formula.base import MSSQLTestBase


class TestMathFunctionMSSQL(MSSQLTestBase, DefaultMathFunctionFormulaConnectorTestSuite):
    supports_atan_2_in_origin = False
