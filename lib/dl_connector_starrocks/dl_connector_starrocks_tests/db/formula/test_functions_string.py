from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestStringFunctionStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultStringFunctionFormulaConnectorTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultStringFunctionFormulaConnectorTestSuite.test_lower_upper: "StarRocks LOWER/UPPER doesn't handle Cyrillic (collation issue)",
            DefaultStringFunctionFormulaConnectorTestSuite.test_icontains_simple: "StarRocks LOWER doesn't handle Cyrillic (collation issue)",
            DefaultStringFunctionFormulaConnectorTestSuite.test_istartswith_simple: "StarRocks LOWER doesn't handle Cyrillic (collation issue)",
            DefaultStringFunctionFormulaConnectorTestSuite.test_iendswith_simple: "StarRocks LOWER doesn't handle Cyrillic (collation issue)",
        },
    )
