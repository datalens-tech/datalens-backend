import sqlalchemy as sa

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

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


# STR


class TestStrTypeFunctionTrino(TrinoFormulaTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0.0"

    def test_str_from_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STR([arr_int_value])", from_=data_table) == "[0,23,456,NULL]"
        assert dbe.eval("STR([arr_float_value])", from_=data_table) == "[0.0,45.0,0.123,NULL]"
        assert dbe.eval("STR([arr_str_value])", from_=data_table) == "['','','cde',NULL]"


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


class TestDateTypeFunctionTrino(TrinoFormulaTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionTrino(
    TrinoFormulaTestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionTrino(TrinoFormulaTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionTrino(TrinoFormulaTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite):
    pass
