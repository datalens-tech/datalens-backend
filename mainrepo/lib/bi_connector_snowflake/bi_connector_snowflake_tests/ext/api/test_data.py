from bi_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataResultTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataDistinctTestSuite,
)
from bi_connector_snowflake_tests.ext.api.base import SnowFlakeDataApiTestBase


class TestSnowFlakeDataResult(SnowFlakeDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestSnowFlakeDataRange(SnowFlakeDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestSnowFlakeDataDistinct(SnowFlakeDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass
