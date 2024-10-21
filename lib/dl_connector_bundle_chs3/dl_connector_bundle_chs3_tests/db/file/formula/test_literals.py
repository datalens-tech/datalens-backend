from dl_connector_bundle_chs3_tests.db.file.formula.base import FileTestBase
from dl_connector_clickhouse.formula.testing.test_suites import LiteralFunctionClickHouseTestSuite


class TestLiteralFunctionFile(FileTestBase, LiteralFunctionClickHouseTestSuite):
    pass
