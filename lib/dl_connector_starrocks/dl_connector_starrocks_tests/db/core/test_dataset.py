import pytest

from dl_core_testing.database import DbTable
from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite

from dl_connector_starrocks.core.constants import SOURCE_TYPE_STARROCKS_TABLE
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks
import dl_connector_starrocks_tests.db.config as test_config
from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass


class TestStarRocksDataset(BaseStarRocksTestClass, DefaultDatasetTestSuite[ConnectionStarRocks]):
    source_type = SOURCE_TYPE_STARROCKS_TABLE

    @pytest.fixture(scope="function")
    def dsrc_params(self, dataset_table: DbTable) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.CATALOG,
            schema_name=dataset_table.db.name,
            table_name=dataset_table.name,
        )
