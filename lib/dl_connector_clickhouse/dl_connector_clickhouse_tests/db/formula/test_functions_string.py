from dl_connector_clickhouse.formula.testing.test_suites import StringFunctionClickHouseTestSuite
from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class TestStringFunctionClickHouse_21_8(ClickHouse_21_8TestBase, StringFunctionClickHouseTestSuite):
    supports_regex_extract_all = True
