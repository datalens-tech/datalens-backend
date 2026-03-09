from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite

from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksDataset(BaseStarRocksTestClass, DefaultDatasetTestSuite[ConnectionStarRocks]):
    pass
