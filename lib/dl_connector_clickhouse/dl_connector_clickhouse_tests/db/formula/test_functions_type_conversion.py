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
from dl_connector_clickhouse_tests.db.formula.base import ClickHouse21p8TestBase

# STR


class TestStrTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionClickHouse21p8(ClickHouse21p8TestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class TestGenericDatetimeParseTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class TestDbCastTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionClickHouse21p8(
    ClickHouse21p8TestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
