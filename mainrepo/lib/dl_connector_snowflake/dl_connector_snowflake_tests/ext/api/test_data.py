from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_connector_snowflake_tests.ext.api.base import SnowFlakeDataApiTestBase
from dl_testing.regulated_test import RegulatedTestParams


class TestSnowFlakeDataResult(SnowFlakeDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "SnowFlake doesn't support arrays",
        }
    )


class TestSnowFlakeDataRange(SnowFlakeDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestSnowFlakeDataDistinct(SnowFlakeDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass
