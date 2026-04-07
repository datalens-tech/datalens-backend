from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestOperatorsStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
    supports_string_int_multiplication = True
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultOperatorFormulaConnectorTestSuite.test_addition_date_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_addition_datetime_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_addition_genericdatetime_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_subtraction_date_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_subtraction_datetime_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_subtraction_genericdatetime_number: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_subtraction_date_time_date_time: "TODO: BI-7171",
            DefaultOperatorFormulaConnectorTestSuite.test_subtraction_genericdatetime_genericdatetime: "TODO: BI-7171",
        },
    )
