from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestLiteralFunctionStarRocks(StarRocksTestBase, DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = False
    supports_custom_tz = False
