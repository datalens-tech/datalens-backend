from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestQuerySettings(DefaultApiTestBase):
    def test_query_settings_defaults_to_empty(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """query_settings defaults to {} in the API response."""
        dataset = control_api.get_dataset(saved_dataset.id).dataset
        assert dataset.query_settings == {}

    def test_query_settings_round_trip(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """query_settings is persisted and returned correctly via save/get."""
        settings = {"max_threads": "4", "max_memory_usage": "1000000000"}
        saved_dataset.query_settings = settings
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        loaded = control_api.get_dataset(saved_dataset.id).dataset
        assert loaded.query_settings == settings

    def test_query_settings_in_options(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """query_settings_enabled is present and boolean in dataset options."""
        response = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id)).json
        assert "query_settings_enabled" in response["options"]
        assert isinstance(response["options"]["query_settings_enabled"], bool)
