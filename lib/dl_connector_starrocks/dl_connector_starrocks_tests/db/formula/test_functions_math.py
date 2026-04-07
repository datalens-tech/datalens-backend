from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestMathFunctionStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultMathFunctionFormulaConnectorTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultMathFunctionFormulaConnectorTestSuite.test_greatest: "TODO: BI-7171 GREATEST returns int for Date args",
            DefaultMathFunctionFormulaConnectorTestSuite.test_least: "TODO: BI-7171 LEAST returns int for Date args",
        },
    )
