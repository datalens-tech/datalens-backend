from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
)


class DateTimeFunctionClickHouseTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_dateadd_non_const_unit_num = True
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetrunc_3 = True
    supports_datetimetz = True


class TestDateTimeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DateTimeFunctionClickHouseTestSuite):
    pass


class TestDateTimeFunctionClickHouse_22_10(ClickHouse_22_10TestBase, DateTimeFunctionClickHouseTestSuite):
    pass
