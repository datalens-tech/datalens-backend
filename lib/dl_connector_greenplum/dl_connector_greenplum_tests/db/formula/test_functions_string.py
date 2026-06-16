from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


GP_ROW_ORDER_SKIP_REASON = "Greenplum row order is non-deterministic without ORDER BY"


class TestStringFunctionGreenplum(GreenplumTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = True

    test_params = RegulatedTestParams(
        mark_tests_skipped={
            # These tests depend on specific row values which are non-deterministic in Greenplum
            DefaultStringFunctionFormulaConnectorTestSuite.test_lower_upper: GP_ROW_ORDER_SKIP_REASON,
            DefaultStringFunctionFormulaConnectorTestSuite.test_icontains_simple: GP_ROW_ORDER_SKIP_REASON,
            DefaultStringFunctionFormulaConnectorTestSuite.test_istartswith_simple: GP_ROW_ORDER_SKIP_REASON,
            DefaultStringFunctionFormulaConnectorTestSuite.test_iendswith_simple: GP_ROW_ORDER_SKIP_REASON,
        },
    )
