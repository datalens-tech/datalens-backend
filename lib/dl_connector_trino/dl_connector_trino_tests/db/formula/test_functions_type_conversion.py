from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


# from dl_formula_testing.testcases.functions_type_conversion import (
#     DefaultDateTypeFunctionFormulaConnectorTestSuite,
#     DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
# )


# STR


class TestStrTypeFunctionTrino(TrinoFormulaTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0.0"


# FLOAT


class TestFloatTypeFunctionTrino(TrinoFormulaTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionTrino(TrinoFormulaTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionTrino(TrinoFormulaTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


# class TestDateTypeFunctionTrino(TrinoFormulaTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
#     pass


# GENERICDATETIME (& DATETIME)


# class TestGenericDatetimeTypeFunctionTrino(
#     TrinoFormulaTestBase,
#     DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
# ):
#     pass
