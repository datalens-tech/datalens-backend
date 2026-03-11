from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite

from dl_connector_starrocks.core.constants import SOURCE_TYPE_STARROCKS_TABLE
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksDataset(BaseStarRocksTestClass, DefaultDatasetTestSuite[ConnectionStarRocks]):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
