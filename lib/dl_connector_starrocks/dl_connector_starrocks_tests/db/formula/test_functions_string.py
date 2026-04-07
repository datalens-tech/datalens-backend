from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestStringFunctionStarRocks(StarRocksTestBase, RegulatedTestCase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_regex_extract_all = False
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultStringFunctionFormulaConnectorTestSuite.test_contains_extended: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_notcontains_extended: "TODO: BI-7171",
        },
        mark_tests_failed={
            DefaultStringFunctionFormulaConnectorTestSuite.test_endswith_simple: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_icontains_simple: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_istartswith_simple: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_iendswith_simple: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_lower_upper: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_regexp_extract_nth: "TODO: BI-7171",
            DefaultStringFunctionFormulaConnectorTestSuite.test_find: "TODO: BI-7171",
        },
    )
