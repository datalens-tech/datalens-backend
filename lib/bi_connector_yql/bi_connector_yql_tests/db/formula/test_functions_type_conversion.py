from dl_formula_testing.evaluator import DbEvaluator
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
from dl_formula_testing.util import to_str

from bi_connector_yql_tests.db.formula.base import YQLTestBase

# STR


class TestStrTypeFunctionYQL(YQLTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0"
    skip_custom_tz = True

    def test_str_from_datetime(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval("STR(#2019-01-02 03:04:05#)")) == "2019-01-02T03:04:05Z"


# FLOAT


class TestFloatTypeFunctionYQL(YQLTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionYQL(YQLTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionYQL(YQLTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionYQL(YQLTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionYQL(
    YQLTestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionYQL(YQLTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionYQL(YQLTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite):
    pass
