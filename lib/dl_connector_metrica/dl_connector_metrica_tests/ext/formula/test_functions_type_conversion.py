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

from dl_connector_metrica_tests.ext.formula.base import MetricaTestBase


# STR


class TestStrTypeFunctionBigQuery(MetricaTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionBigQuery(MetricaTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionBigQuery(MetricaTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionBigQuery(MetricaTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionBigQuery(MetricaTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionBigQuery(
    MetricaTestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionBigQuery(
    MetricaTestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionBigQuery(
    MetricaTestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass
