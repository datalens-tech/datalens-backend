import abc

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_testing.dataset_base import DatasetTestBase


class DefaultConnectorDatasetTestSuite(DatasetTestBase, metaclass=abc.ABCMeta):
    def check_basic_dataset(self, ds: Dataset) -> None:
        """Additional dataset checks can be defined here"""
        assert ds.id
        assert len(ds.result_schema)

        field_names = {field.title for field in ds.result_schema}
        assert {'category', 'city'}.issubset(field_names)

    def test_create_basic_dataset(
            self, saved_dataset: Dataset,
    ) -> None:
        self.check_basic_dataset(saved_dataset)
