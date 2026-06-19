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
    MySQL5p7TestBase,
    MySQL8p0p12TestBase,
)

# STR


class TestStrTypeFunctionMySQL5p7(MySQL5p7TestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True


class TestStrTypeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    skip_custom_tz = True


# FLOAT


class TestFloatTypeFunctionMySQL5p7(MySQL5p7TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


class TestFloatTypeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionMySQL5p7(MySQL5p7TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


class TestBoolTypeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionMySQL5p7(MySQL5p7TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


class TestIntTypeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionMySQL5p7(MySQL5p7TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


class TestDateTypeFunctionMySQL8p0p12(MySQL8p0p12TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionMySQL5p7(
    MySQL5p7TestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGenericDatetimeTypeFunctionMySQL8p0p12(
    MySQL8p0p12TestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionMySQL5p7(
    MySQL5p7TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopointTypeFunctionMySQL8p0p12(
    MySQL8p0p12TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionMySQL5p7(
    MySQL5p7TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopolygonTypeFunctionMySQL8p0p12(
    MySQL8p0p12TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass
