from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


class LogicalFunctionGreenplumTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True


class TestLogicalFunctionGreenplum(GreenplumTestBase, LogicalFunctionGreenplumTestSuite):
    pass
