from dl_connector_clickhouse.formula.testing.test_suites import DateTimeFunctionClickHouseTestSuite
from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse21p8TestBase,
    ClickHouse22p10TestBase,
)


class TestDateTimeFunctionClickHouse21p8(ClickHouse21p8TestBase, DateTimeFunctionClickHouseTestSuite):
    pass


class TestDateTimeFunctionClickHouse22p10(ClickHouse22p10TestBase, DateTimeFunctionClickHouseTestSuite):
    pass
