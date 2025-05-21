import abc
from typing import Optional

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib.app_settings import ControlApiAppSettings
from dl_api_lib.enums import DatasetAction
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.api_constants import DLHeadersCommon
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_testing.regulated_test import RegulatedTestCase

import pytest


class DefaultConnectorDatasetTestSuite(DatasetTestBase, RegulatedTestCase, metaclass=abc.ABCMeta):
    def check_basic_dataset(self, ds: Dataset) -> None:
        """Additional dataset checks can be defined here"""
        assert ds.id
        assert ds.load_preview_by_default
        assert not ds.template_enabled
        assert not ds.data_export_forbidden
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
                ],
            ).dataset
            assert dataset.sources
            assert all(source.connection_id == new_connection_id for source in dataset.sources)

    @pytest.fixture(scope="function")
    def export_import_headers(self, control_api_app_settings: ControlApiAppSettings) -> dict[str, str]:
        return {
            DLHeadersCommon.US_MASTER_TOKEN.value: control_api_app_settings.US_MASTER_TOKEN,
        }

    def test_export_import_invalid_schema(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        export_import_headers: dict[str, str],
    ):
        export_data = dict()
        export_resp = control_api.export_dataset(dataset=saved_dataset, data=export_data, headers=export_import_headers)
        assert export_resp.status_code == 400, export_resp.json

        import_data = dict()
        import_resp = control_api.import_dataset(data=import_data, headers=export_import_headers)
        assert import_resp.status_code == 400, import_resp.json

        import_data = {"id_mapping": {}}
        import_resp = control_api.import_dataset(data=import_data, headers=export_import_headers)
        assert import_resp.status_code == 400, import_resp.json

    def test_export_import_dataset(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        saved_dataset: Dataset,
        export_import_headers: dict[str, str],
    ) -> None:
        # test common export
        export_data = {"id_mapping": {saved_connection_id: "conn_id_1"}}
        export_resp = control_api.export_dataset(dataset=saved_dataset, data=export_data, headers=export_import_headers)
        assert export_resp.status_code == 200
        assert export_resp.json["dataset"]["sources"][0]["connection_id"] == "conn_id_1"

        # test common import
        import_data: dict = {
            "id_mapping": {"conn_id_1": saved_connection_id},
            "data": {"workbook_id": None, "dataset": export_resp.json["dataset"]},
        }
        import_resp = control_api.import_dataset(data=import_data, headers=export_import_headers)
        assert import_resp.status_code == 200, f"{import_resp.json} vs {export_resp.json}"

        control_api.delete_dataset(dataset_id=import_resp.json["id"])

    def test_export_import_dataset_with_no_connection(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sync_us_manager: SyncUSManager,
        saved_dataset: Dataset,
        export_import_headers: dict[str, str],
    ):
        sync_us_manager.delete(sync_us_manager.get_by_id(saved_connection_id))

        # export with no connection
        export_req_data = {"id_mapping": {}}
        export_resp = control_api.export_dataset(dataset=saved_dataset, data=export_req_data, headers=export_import_headers)
        assert export_resp.status_code == 200, export_resp.json
        assert export_resp.json["dataset"]["sources"][0]["connection_id"] == None

        # import with no connection
        import_req_data: dict = {
            "id_mapping": {},
            "data": {"workbook_id": None, "dataset": export_resp.json["dataset"]},
        }
        import_resp = control_api.import_dataset(data=import_req_data, headers=export_import_headers)
        assert import_resp.status_code == 200, f"{import_resp.json} vs {export_resp.json}"

        control_api.delete_dataset(dataset_id=import_resp.json["id"])
