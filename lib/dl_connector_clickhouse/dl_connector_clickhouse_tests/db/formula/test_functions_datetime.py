from dl_connector_clickhouse.formula.testing.test_suites import DateTimeFunctionClickHouseTestSuite
from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
)


class TestDateTimeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DateTimeFunctionClickHouseTestSuite):
    pass


class TestDateTimeFunctionClickHouse_22_10(ClickHouse_22_10TestBase, DateTimeFunctionClickHouseTestSuite):
    pass
