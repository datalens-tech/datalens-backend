from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_snowflake_tests.ext.api.base import SnowFlakeDatasetTestBase


class TestSnowFlakeDataset(SnowFlakeDatasetTestBase, DefaultConnectorDatasetTestSuite):
    def check_basic_dataset(self, ds: Dataset) -> None:
        field_names = {field.title for field in ds.result_schema}
        assert {"Category", "City"}.issubset(field_names)
