from bi_formula.connectors.base.testing.functions_window import (
    DefaultWindowFunctionFormulaConnectorTestSuite,
)
from bi_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_22_10TestBase
)


class TestWindowFunctionClickHouse_22_10(ClickHouse_22_10TestBase, DefaultWindowFunctionFormulaConnectorTestSuite):
    pass
