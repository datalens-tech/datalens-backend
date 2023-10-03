from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from bi_connector_gsheets_tests.ext.api.base import GsheetsDatasetTestBase


class TestGsheetsDataset(GsheetsDatasetTestBase, DefaultConnectorDatasetTestSuite):
    def check_basic_dataset(self, ds: Dataset) -> None:
        assert ds.id
        assert len(ds.result_schema)

        field_names = {field.title for field in ds.result_schema}
        assert {"codename", "series", "created", "release"}.issubset(field_names)
