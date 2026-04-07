from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestAggFunctionsStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_all_concat = False
    supports_quantile = False
    supports_median = False
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultMainAggFunctionFormulaConnectorTestSuite.test_date_avg_function: "TODO: BI-7171 AVG not defined for DATE type in StarRocks dialect",
            DefaultMainAggFunctionFormulaConnectorTestSuite.test_datetime_avg_function: "TODO: BI-7171 AVG not defined for DATETIME type in StarRocks dialect",
        },
    )
