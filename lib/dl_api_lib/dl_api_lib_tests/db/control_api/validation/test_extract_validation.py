import http

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib.app_settings import ControlApiAppSettings
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.api_constants import DLHeadersCommon
from dl_constants.enums import (
    ExtractMode,
    ExtractStatus,
    OrderDirection,
    WhereClauseOperation,
)
from dl_core.base_models import DefaultWhereClause
from dl_core.fields import (
    FilterField,
    OrderField,
)
from dl_core.us_extract import ExtractProperties

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE


class TestExtractValidation(DefaultApiTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )

    def test_save_dataset_without_extract_uses_defaults(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        dataset_params: dict,
        annotation: dict,
    ):
        dataset = self.make_basic_dataset(
            control_api=control_api,
            connection_id=saved_connection_id,
            dataset_params=dataset_params,
            annotation=annotation,
        )

        saved_dataset = control_api.save_dataset(dataset).dataset

        try:
            assert saved_dataset.extract == ExtractProperties(
                mode=ExtractMode.disabled,
                status=ExtractStatus.disabled,
                filters=[],
                errors=[],
                last_update=0,
                sorting=[],
            )
        finally:
            control_api.delete_dataset(dataset_id=saved_dataset.id, fail_ok=False)

    def test_save_dataset_with_valid_extract_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [{"id": "filter_1", "guid": field_guids[0], "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        saved_dataset = control_api.save_dataset(ds_resp.dataset).dataset

        assert saved_dataset.extract == ExtractProperties(
            mode=ExtractMode.manual,
            status=ExtractStatus.disabled,
            filters=[
                FilterField(id="filter_1", guid=field_guids[0], filters=[]),
            ],
            sorting=[
                OrderField(id="sort_1", guid=field_guids[0], order=OrderDirection.asc),
            ],
            errors=[],
            last_update=0,
        )

    def test_save_dataset_with_invalid_sorting_fields_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [],
                "sorting": [{"id": "invalid_sort", "guid": "non-existent-guid", "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"

        dataset = ds_resp.dataset
        invalid_sort_field = next((field for field in dataset.extract.sorting if field.id == "invalid_sort"), None)
        assert invalid_sort_field is not None
        assert invalid_sort_field.valid is False

        errors = [error for error in dataset.component_errors.items if error.id == "invalid_sort"]
        assert len(errors) == 1
        assert errors[0].errors[0].code == "ERR.DS_API.VALIDATION.ERROR.EXTRACT.SORTING_FIELD_MISSING"

    def test_save_dataset_with_invalid_filter_fields_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [{"id": "invalid_filter", "guid": "non-existent-guid", "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"

        dataset = ds_resp.dataset
        invalid_sort_field = next((field for field in dataset.extract.filters if field.id == "invalid_filter"), None)
        assert invalid_sort_field is not None
        assert invalid_sort_field.valid is False

        errors = [error for error in dataset.component_errors.items if error.id == "invalid_filter"]
        assert len(errors) == 1
        assert errors[0].errors[0].code == "ERR.DS_API.VALIDATION.ERROR.EXTRACT.FILTER_FIELD_MISSING"

    def test_control_api_returns_extract_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [{"id": "filter_1", "guid": field_guids[0], "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK

        saved_dataset = control_api.save_dataset(ds_resp.dataset).dataset

        retrieved_dataset = control_api.get_dataset(saved_dataset.id).dataset

        assert retrieved_dataset.extract == ExtractProperties(
            mode=ExtractMode.manual,
            status=ExtractStatus.disabled,
            filters=[
                FilterField(id="filter_1", guid=field_guids[0], filters=[]),
            ],
            sorting=[
                OrderField(id="sort_1", guid=field_guids[0], order=OrderDirection.asc),
            ],
            errors=[],
            last_update=0,
        )

    def test_update_extract_with_existing_fields_succeeds(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK

        updated_extract = {
            "action": "update_extract",
            "extract": {
                "mode": "automatic",
                "filters": [{"id": "filter_1", "guid": field_guids[0], "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "desc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[updated_extract],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK
        assert ds_resp.dataset.extract == ExtractProperties(
            mode=ExtractMode.automatic,
            status=ExtractStatus.disabled,
            filters=[
                FilterField(id="filter_1", guid=field_guids[0], filters=[]),
            ],
            sorting=[
                OrderField(id="sort_1", guid=field_guids[0], order=OrderDirection.desc),
            ],
            errors=[],
            last_update=0,
        )

    def test_update_extract_with_invalid_sorting_fields_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [],
                "sorting": [{"id": "valid_sort", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK

        invalid_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [],
                "sorting": [{"id": "invalid_sort", "guid": "non-existent-guid", "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[invalid_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST

        dataset = ds_resp.dataset
        invalid_sort_field = next((field for field in dataset.extract.sorting if field.id == "invalid_sort"), None)
        assert invalid_sort_field is not None
        assert invalid_sort_field.valid is False

        errors = [error for error in dataset.component_errors.items if error.id == "invalid_sort"]
        assert len(errors) == 1
        assert errors[0].errors[0].code == "ERR.DS_API.VALIDATION.ERROR.EXTRACT.SORTING_FIELD_MISSING"

    def test_update_extract_with_invalid_filter_fields_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        field_guids = [field.id for field in saved_dataset.result_schema]

        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [{"id": "valid_filter", "guid": field_guids[0], "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK

        invalid_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [{"id": "invalid_filter", "guid": "non-existent-guid", "filters": []}],
                "sorting": [{"id": "sort_1", "guid": field_guids[0], "order": "asc"}],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[invalid_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST

        dataset = ds_resp.dataset
        invalid_filter_field = next((field for field in dataset.extract.filters if field.id == "invalid_filter"), None)
        assert invalid_filter_field is not None
        assert invalid_filter_field.valid is False

        errors = [error for error in dataset.component_errors.items if error.id == "invalid_filter"]
        assert len(errors) == 1
        assert errors[0].errors[0].code == "ERR.DS_API.VALIDATION.ERROR.EXTRACT.FILTER_FIELD_MISSING"

    def test_extract_enabled_requires_non_empty_sorting(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        extract_update = {
            "action": "update_extract",
            "extract": {
                "mode": "manual",
                "filters": [],
                "sorting": [],
            },
        }

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[extract_update],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL.EXTRACT.SORTING_EMPTY"

    def test_get_dataset_by_revision_returns_all_extract_fields(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        dataset_params: dict,
        annotation: dict,
    ):
        # Create a basic dataset
        dataset = self.make_basic_dataset(
            control_api=control_api,
            connection_id=saved_connection_id,
            dataset_params=dataset_params,
            annotation=annotation,
        )

        # Save the dataset to get an initial revision
        saved_dataset = control_api.save_dataset(dataset).dataset
        field_guids = [field.id for field in saved_dataset.result_schema]

        try:
            # Configure extract settings
            extract_update = {
                "action": "update_extract",
                "extract": {
                    "mode": "manual",
                    "filters": [
                        {
                            "id": "filter_1",
                            "guid": field_guids[0],
                            "filters": [
                                {
                                    "operation": "EQ",
                                    "values": ["test_value"],
                                },
                            ],
                        },
                    ],
                    "sorting": [
                        {
                            "id": "sort_1",
                            "guid": field_guids[0],
                            "order": "asc",
                        },
                    ],
                },
            }

            # Apply extract settings
            ds_resp = control_api.apply_updates(
                dataset=saved_dataset,
                updates=[extract_update],
                fail_ok=False,
            )
            assert ds_resp.status_code == http.HTTPStatus.OK

            final_saved_dataset = control_api.save_dataset(ds_resp.dataset).dataset

            # Check that API returns all fields as expected
            retrieved_dataset = control_api.get_dataset(final_saved_dataset.id).dataset
            extract = retrieved_dataset.extract

            # Validate extract settings
            assert extract == ExtractProperties(
                mode=ExtractMode.manual,
                status=ExtractStatus.disabled,
                filters=[
                    FilterField(
                        id="filter_1",
                        guid=field_guids[0],
                        filters=[
                            DefaultWhereClause(
                                operation=WhereClauseOperation.EQ,
                                values=["test_value"],
                            ),
                        ],
                        valid=True,
                    ),
                ],
                sorting=[
                    OrderField(
                        id="sort_1",
                        guid=field_guids[0],
                        order=OrderDirection.asc,
                        valid=True,
                    ),
                ],
                errors=[],
                last_update=0,
            )

        finally:
            control_api.delete_dataset(dataset_id=saved_dataset.id, fail_ok=True)


class TestExtractExportImport(DefaultApiTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self, sample_table) -> dict:
        return dict(
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )

    @pytest.fixture(scope="function")
    def export_import_headers(self, control_api_app_settings: ControlApiAppSettings) -> dict[str, str]:
        assert control_api_app_settings.US_MASTER_TOKEN is not None
        return {
            DLHeadersCommon.US_MASTER_TOKEN.value: control_api_app_settings.US_MASTER_TOKEN,
        }

    def test_export_does_not_include_extract(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        saved_dataset: Dataset,
        export_import_headers: dict[str, str],
    ) -> None:
        export_resp = control_api.export_dataset(
            dataset=saved_dataset,
            data={
                "id_mapping": {
                    saved_connection_id: "conn_id_1",
                },
            },
            headers=export_import_headers,
        )

        assert export_resp.status_code == http.HTTPStatus.OK
        assert "extract" not in export_resp.json["dataset"]

    def test_import_ignores_extract_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        saved_dataset: Dataset,
        export_import_headers: dict[str, str],
    ) -> None:
        export_resp = control_api.export_dataset(
            dataset=saved_dataset,
            data={
                "id_mapping": {
                    saved_connection_id: "conn_id_1",
                },
            },
            headers=export_import_headers,
        )
        assert export_resp.status_code == http.HTTPStatus.OK

        dataset_body = export_resp.json["dataset"]
        dataset_body["extract"] = {
            "mode": "manual",
            "filters": [],
            "sorting": [
                {
                    "id": "sort_1",
                    "guid": "some-guid",
                    "order": "asc",
                },
            ],
        }

        import_resp = control_api.import_dataset(
            data={
                "id_mapping": {
                    "conn_id_1": saved_connection_id,
                },
                "data": {
                    "workbook_id": None,
                    "dataset": dataset_body,
                },
            },
            headers=export_import_headers,
        )
        assert import_resp.status_code == http.HTTPStatus.OK

        imported_dataset = control_api.get_dataset(import_resp.json["id"]).dataset
        try:
            assert imported_dataset.extract == ExtractProperties(
                mode=ExtractMode.disabled,
                status=ExtractStatus.disabled,
                filters=[],
                sorting=[],
                errors=[],
                last_update=0,
            )
        finally:
            control_api.delete_dataset(dataset_id=imported_dataset.id, fail_ok=True)
