from bi_connector_mssql_tests.db.formula.base import (
    MSSQLTestBase,
)
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
)


# STR

class TestStrTypeFunctionMSSQL(MSSQLTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = '0.0E0'
    skip_custom_tz = True


# FLOAT

class TestFloatTypeFunctionMSSQL(MSSQLTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL

class TestBoolTypeFunctionMSSQL(MSSQLTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT

class TestIntTypeFunctionMSSQL(MSSQLTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE

class TestDateTypeFunctionMSSQL(MSSQLTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)

class TestGenericDatetimeTypeFunctionMSSQL(
        MSSQLTestBase, DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT

class TestGeopointTypeFunctionMSSQL(
        MSSQLTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON

class TestGeopolygonTypeFunctionMSSQL(
        MSSQLTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass
