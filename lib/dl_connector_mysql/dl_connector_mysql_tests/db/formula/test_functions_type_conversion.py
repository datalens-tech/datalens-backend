from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_mysql_tests.db.formula.base import (
    MySQL_5_7TestBase,
    MySQL_8_0_12TestBase,
)


# STR


class TestStrTypeFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True


class TestStrTypeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True


# FLOAT


class TestFloatTypeFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


class TestFloatTypeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


class TestBoolTypeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


class TestIntTypeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionMySQL_5_7(MySQL_5_7TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


class TestDateTypeFunctionMySQL_8_0_12(MySQL_8_0_12TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGenericDatetimeTypeFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopointTypeFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionMySQL_5_7(
    MySQL_5_7TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopolygonTypeFunctionMySQL_8_0_12(
    MySQL_8_0_12TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass
