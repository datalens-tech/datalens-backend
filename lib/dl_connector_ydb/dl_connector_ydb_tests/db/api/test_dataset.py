from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_ydb_tests.db.api.base import YDBDatasetTestBase
from dl_connector_ydb_tests.db.config import TABLE_SCHEMA


class TestYDBDataset(YDBDatasetTestBase, DefaultConnectorDatasetTestSuite):
    def check_basic_dataset(self, ds: Dataset, annotation: dict) -> None:
        assert ds.id
        field_names = {field.title for field in ds.result_schema}
        assert field_names == {column[0] for column in TABLE_SCHEMA}

        assert ds.annotation == annotation
