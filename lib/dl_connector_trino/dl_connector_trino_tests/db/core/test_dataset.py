from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite

from dl_connector_trino.core.constants import SOURCE_TYPE_TRINO_TABLE
from dl_connector_trino.core.us_connection import ConnectionTrino
from dl_connector_trino_tests.db.core.base import BaseTrinoTestClass


class TestTrinoDataset(BaseTrinoTestClass, DefaultDatasetTestSuite[ConnectionTrino]):
    source_type = SOURCE_TYPE_TRINO_TABLE
