from bi_formula_testing.testcases.functions_type_conversion import (
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
)

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


# STR

class TestStrTypeFunctionOracle(OracleTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = '0'
    skip_custom_tz = True


# FLOAT

class TestFloatTypeFunctionOracle(OracleTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL

class TestBoolTypeFunctionOracle(OracleTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT

class TestIntTypeFunctionOracle(OracleTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE

class TestDateTypeFunctionOracle(OracleTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)

class TestGenericDatetimeTypeFunctionOracle(
        OracleTestBase, DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT

class TestGeopointTypeFunctionOracle(
        OracleTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON

class TestGeopolygonTypeFunctionOracle(
        OracleTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass
