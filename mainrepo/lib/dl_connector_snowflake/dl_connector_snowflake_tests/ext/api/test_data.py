from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)

from dl_connector_snowflake_tests.ext.api.base import SnowFlakeDataApiTestBase


class TestSnowFlakeDataResult(SnowFlakeDataApiTestBase, DefaultConnectorDataResultTestSuite):
    pass


class TestSnowFlakeDataRange(SnowFlakeDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestSnowFlakeDataDistinct(SnowFlakeDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass
