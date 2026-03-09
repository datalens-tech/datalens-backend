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

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestStrTypeFunctionStarRocks(StarRocksTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True


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


class TestTreeTypeFunctionStarRocks(StarRocksTestBase, DefaultTreeTypeFunctionFormulaConnectorTestSuite):
    pass
