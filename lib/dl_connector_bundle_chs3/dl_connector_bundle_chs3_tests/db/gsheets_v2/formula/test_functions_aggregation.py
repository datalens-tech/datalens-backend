from dl_connector_bundle_chs3_tests.db.gsheets_v2.formula.base import GSheetsTestBase
from dl_connector_clickhouse.formula.testing.test_suites import MainAggFunctionClickHouseTestSuite


class TestMainAggFunctionGSheets(GSheetsTestBase, MainAggFunctionClickHouseTestSuite):
    pass
