from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestDatasetVersionItemResponseSchema(DefaultApiTestBase):
    def test_dataset_version_item_response_schema(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Make sure that all dataset fields have the expected structure."""
        dataset = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id)).json

        assert dataset["id"] == saved_dataset.id
        assert dataset["is_favorite"] is False

        assert isinstance(dataset["permissions"], dict)
        assert all((isinstance(permission, bool) for permission in dataset["permissions"].values()))

        assert isinstance(dataset["full_permissions"], dict)
        assert all((isinstance(permission, bool) for permission in dataset["full_permissions"].values()))

        assert isinstance(dataset["dataset"], dict)
        assert isinstance(dataset["options"], dict)
