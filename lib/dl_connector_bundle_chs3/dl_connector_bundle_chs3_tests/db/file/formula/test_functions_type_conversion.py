from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_bundle_chs3_tests.db.file.formula.base import FileTestBase
from dl_connector_clickhouse.formula.testing.test_suites import (
    DateTypeFunctionClickHouseTestSuite,
    DbCastTypeFunctionClickHouseTestSuite,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
    StrTypeFunctionClickHouseTestSuite,
)


# STR


class TestStrTypeFunctionFile(FileTestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionFile(FileTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionFile(FileTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionFile(FileTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionFile(FileTestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionFile(FileTestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionFile(
    FileTestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class TestGenericDatetimeParseTypeFunctionFile(
    FileTestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionFile(
    FileTestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionFile(
    FileTestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class TestDbCastTypeFunctionFile(
    FileTestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionFile(
    FileTestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
