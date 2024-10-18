from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)

from dl_connector_bundle_chs3_tests.db.yadocs.formula.base import YaDocsTestBase
from dl_connector_clickhouse.formula.testing.test_suites import (
    DateTypeFunctionClickHouseTestSuite,
    DbCastTypeFunctionClickHouseTestSuite,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
    StrTypeFunctionClickHouseTestSuite,
)


# STR


class TestStrTypeFunctionYaDocs(YaDocsTestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionYaDocs(YaDocsTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionYaDocs(YaDocsTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionYaDocs(YaDocsTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionYaDocs(YaDocsTestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionYaDocs(YaDocsTestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionYaDocs(
    YaDocsTestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class TestGenericDatetimeParseTypeFunctionYaDocs(
    YaDocsTestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionYaDocs(
    YaDocsTestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionYaDocs(
    YaDocsTestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class TestDbCastTypeFunctionYaDocs(
    YaDocsTestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionYaDocs(
    YaDocsTestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
