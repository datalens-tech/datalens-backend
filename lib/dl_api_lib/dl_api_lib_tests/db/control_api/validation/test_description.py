from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestDescription(DefaultApiTestBase):
    def test_change_description(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        description = "New description"

        saved_dataset = control_api.update_description(saved_dataset, description).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert saved_dataset.annotation["description"] == description, "Description should be updated"

        saved_dataset = control_api.get_dataset(saved_dataset.id).dataset
        assert saved_dataset.annotation["description"] == description, "Description should be saved"
