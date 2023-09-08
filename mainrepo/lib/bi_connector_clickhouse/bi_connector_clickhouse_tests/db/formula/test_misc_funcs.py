from bi_formula_testing.testcases.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)

from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class TestMiscFunctionalityClickHouse_21_8(ClickHouse_21_8TestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
