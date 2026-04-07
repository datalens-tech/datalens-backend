from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestDateTimeFunctionStarRocks(
    StarRocksTestBase, RegulatedTestCase, DefaultDateTimeFunctionFormulaConnectorTestSuite
):
    supports_deprecated_dateadd = False
    supports_deprecated_datepart_2 = False
    supports_datetimetz = False
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_dateadd_to_today: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datetime_dateadd_with_uneven_month_lengths: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datetime_dateadd: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datepart_2_const: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datepart_3: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datepart_2_non_const: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_specific_date_part_functions: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_datetrunc_2: "TODO: BI-7171",
            DefaultDateTimeFunctionFormulaConnectorTestSuite.test_dateadd_to_now: "TODO: BI-7171",
        },
    )
