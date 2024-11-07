import abc
from typing import Optional

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib.enums import DatasetAction
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_testing.regulated_test import RegulatedTestCase


class DefaultConnectorDatasetTestSuite(DatasetTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    def check_basic_dataset(self, ds: Dataset) -> None:
        """Additional dataset checks can be defined here"""
        assert ds.id
        assert ds.load_preview_by_default
        assert len(ds.result_schema)

        field_names = {field.title for field in ds.result_schema}
        assert {"category", "city"}.issubset(field_names)

    def test_create_basic_dataset(
        self,
        saved_dataset: Dataset,
    ) -> None:
        self.check_basic_dataset(saved_dataset)

    def test_remove_connection(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        saved_dataset: Dataset,
        sync_us_manager: SyncUSManager,
    ) -> None:
        sync_us_manager.delete(sync_us_manager.get_by_id(saved_connection_id))
        dataset_resp = control_api.load_dataset(saved_dataset)
        assert dataset_resp.status_code == 200, dataset_resp.json

    def test_result_field_available_after_deletion(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
    ) -> None:
        # arrange
        dataset = saved_dataset
        result_field_to_remove = next(iter(dataset.result_schema))

        # act
        dataset_after_update = control_api.apply_updates(dataset, updates=[result_field_to_remove.delete()]).dataset
        dataset_after_deletion = control_api.save_dataset(dataset_after_update).dataset
        dataset_after_reload = control_api.refresh_dataset_sources(
            dataset_after_deletion, dataset.sources._item_ids
        ).dataset

        # assert
        assert result_field_to_remove.title not in [item.title for item in dataset_after_deletion.result_schema]
        assert result_field_to_remove.title in [item.title for item in dataset_after_reload.result_schema]
        assert len(dataset.result_schema) == len(dataset.result_schema)

    def test_replace_connection(
        self,
        saved_dataset: Dataset,
        saved_connection_id: str,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        bi_headers: Optional[dict[str, str]],
        control_api: SyncHttpDatasetApiV1,
    ) -> None:
        with self.create_connection(
            control_api_sync_client=control_api_sync_client,
            connection_params=connection_params,
            bi_headers=bi_headers,
        ) as new_connection_id:
            dataset = control_api.apply_updates(
                saved_dataset,
                updates=[
                    {
                        "action": DatasetAction.replace_connection.value,
                        "connection": {"id": saved_connection_id, "new_id": new_connection_id},
                    },
                ]
            ).dataset
            dataset = control_api.save_dataset(dataset).dataset
            assert dataset.sources
            assert all(source.id == new_connection_id for source in dataset.sources), list(dataset.sources)
