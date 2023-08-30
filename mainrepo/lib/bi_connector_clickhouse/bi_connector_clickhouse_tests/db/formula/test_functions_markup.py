from bi_formula.connectors.base.testing.functions_markup import (
    DefaultMarkupFunctionFormulaConnectorTestSuite,
)

from bi_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase


class TestMarkupFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultMarkupFunctionFormulaConnectorTestSuite):
    pass
