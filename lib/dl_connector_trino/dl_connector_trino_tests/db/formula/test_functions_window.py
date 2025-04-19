from dl_formula_testing.testcases.functions_window import DefaultWindowFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestWindowFunctionTrino(TrinoFormulaTestBase, RegulatedTestCase, DefaultWindowFunctionFormulaConnectorTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultWindowFunctionFormulaConnectorTestSuite.test_sum_if: "FILTER is not yet supported for window functions in Trino",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_count_if: "FILTER is not yet supported for window functions in Trino",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_avg_if: "FILTER is not yet supported for window functions in Trino",
        },
    )
