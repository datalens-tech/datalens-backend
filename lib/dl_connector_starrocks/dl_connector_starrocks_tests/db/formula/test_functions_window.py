from dl_formula_testing.testcases.functions_window import DefaultWindowFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestWindowFunctionStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultWindowFunctionFormulaConnectorTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultWindowFunctionFormulaConnectorTestSuite.test_sum: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_min: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_max: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_count: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_avg: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_sum_if: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_count_if: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_avg_if: "TODO: BI-7171",
            DefaultWindowFunctionFormulaConnectorTestSuite.test_lag: "TODO: BI-7171",
        },
    )
