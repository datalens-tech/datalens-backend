from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_bundle_chs3_tests.db.gsheets_v2.formula.base import GSheetsTestBase
from dl_connector_clickhouse.formula.testing.test_suites import (
    DateTypeFunctionClickHouseTestSuite,
    DbCastTypeFunctionClickHouseTestSuite,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
    StrTypeFunctionClickHouseTestSuite,
)


# STR


class TestStrTypeFunctionGSheets(GSheetsTestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionGSheets(GSheetsTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionGSheets(GSheetsTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionGSheets(GSheetsTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionGSheets(GSheetsTestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionGSheets(GSheetsTestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionGSheets(
    GSheetsTestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class TestGenericDatetimeParseTypeFunctionGSheets(
    GSheetsTestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionGSheets(
    GSheetsTestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionGSheets(
    GSheetsTestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class TestDbCastTypeFunctionGSheets(
    GSheetsTestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionGSheets(
    GSheetsTestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
