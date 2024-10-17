from dl_connector_bundle_chs3_tests.db.yadocs.formula.base import YaDocsTestBase
from dl_connector_clickhouse.formula.testing.test_suites import DateTimeFunctionClickHouseTestSuite


class TestDateTimeFunctionYaDocs(YaDocsTestBase, DateTimeFunctionClickHouseTestSuite):
    pass
