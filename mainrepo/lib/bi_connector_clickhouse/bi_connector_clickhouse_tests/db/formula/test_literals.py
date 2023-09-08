from bi_formula_testing.testcases.literals import (
    DefaultLiteralFormulaConnectorTestSuite,
)

from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class LiteralFunctionClickHouseTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = False
    supports_utc = True
    supports_custom_tz = True
    default_tz = None


class TestLiteralFunctionClickHouse_21_8(ClickHouse_21_8TestBase, LiteralFunctionClickHouseTestSuite):
    pass
