from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_clickhouse.formula.testing.test_suites import (
    DateTypeFunctionClickHouseTestSuite,
    DbCastTypeFunctionClickHouseTestSuite,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
    StrTypeFunctionClickHouseTestSuite,
)
from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


# STR


class TestStrTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class TestGenericDatetimeParseTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class TestDbCastTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
