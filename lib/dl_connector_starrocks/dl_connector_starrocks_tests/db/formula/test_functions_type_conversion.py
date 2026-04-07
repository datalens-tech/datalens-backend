from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)
from dl_testing.regulated_test import (
    RegulatedTestCase,
    RegulatedTestParams,
)

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestStrTypeFunctionStarRocks(StarRocksTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True
    zero_float_to_str_value = "0.0"


class TestFloatTypeFunctionStarRocks(StarRocksTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


class TestBoolTypeFunctionStarRocks(StarRocksTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


class TestIntTypeFunctionStarRocks(StarRocksTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


class TestDateTypeFunctionStarRocks(StarRocksTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


class TestGenericDatetimeTypeFunctionStarRocks(
    StarRocksTestBase, DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite
):
    pass


class TestGeopointTypeFunctionStarRocks(StarRocksTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite):
    pass


class TestGeopolygonTypeFunctionStarRocks(StarRocksTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite):
    pass


class TestTreeTypeFunctionStarRocks(
    StarRocksTestBase, RegulatedTestCase, DefaultTreeTypeFunctionFormulaConnectorTestSuite
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultTreeTypeFunctionFormulaConnectorTestSuite.test_tree_str: "TODO: BI-7171 StarRocks TREE type not supported by type transformer",
        },
    )
