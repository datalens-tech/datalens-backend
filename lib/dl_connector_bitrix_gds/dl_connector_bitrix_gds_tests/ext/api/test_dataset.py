from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite

from dl_connector_bitrix_gds_tests.ext.api.base import (
    BitrixDatasetTestBase,
    BitrixSmartTablesDatasetTestBase,
)


class TestBitrixDataset(BitrixDatasetTestBase, DefaultConnectorDatasetTestSuite):
    def check_basic_dataset(self, ds: Dataset) -> None:
        assert ds.id
        assert len(ds.result_schema)

        field_names = {field.title for field in ds.result_schema}
        assert {"ID", "DATE_CREATE", "DATE_MODIFY", "ASSIGNED_BY_NAME"}.issubset(field_names)


class TestBitrixSmartTablesDataset(BitrixSmartTablesDatasetTestBase, DefaultConnectorDatasetTestSuite):
    def check_basic_dataset(self, ds: Dataset, annotation: dict) -> None:
        assert ds.id
        assert len(ds.result_schema)

        field_names = {field.title for field in ds.result_schema}
        assert {"ID", "UF_CRM_5_1694020695771", "ASSIGNED_BY_NAME"}.issubset(field_names)

        assert ds.annotation == annotation
