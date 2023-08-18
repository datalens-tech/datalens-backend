from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.functions_type_conversion import (
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
)


# STR

class TestStrTypeFunctionSnowFlake(SnowFlakeTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    pass


# FLOAT

class TestFloatTypeFunctionSnowFlake(SnowFlakeTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL

class TestBoolTypeFunctionSnowFlake(SnowFlakeTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT

class TestIntTypeFunctionSnowFlake(SnowFlakeTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE

class TestDateTypeFunctionSnowFlake(SnowFlakeTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)

class TestGenericDatetimeTypeFunctionSnowFlake(
        SnowFlakeTestBase, DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass
