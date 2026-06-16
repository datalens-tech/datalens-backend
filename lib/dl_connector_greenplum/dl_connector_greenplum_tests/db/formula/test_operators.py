from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


GP_ROW_ORDER_SKIP_REASON = "Greenplum row order is non-deterministic without ORDER BY"


class OperatorGreenplumTestSuite(DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
    make_float_array_cast = "double precision[]"
    make_str_array_cast = "text[]"


class TestOperatorGreenplum(GreenplumTestBase, OperatorGreenplumTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            # This test depends on specific row values which are non-deterministic in Greenplum
            DefaultOperatorFormulaConnectorTestSuite.test_addition_array_array: GP_ROW_ORDER_SKIP_REASON,
        },
    )
