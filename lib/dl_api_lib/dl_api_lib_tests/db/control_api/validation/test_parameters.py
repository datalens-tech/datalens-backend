import http

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
import dl_constants.enums as dl_constants_enums


class TestValidationParameters(DefaultApiTestBase):
    def test_add_parameter(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "123",
                        "value_constraint": {
                            "type": "set",
                            "values": ["123", "456"],
                        },
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK, ds_resp.response_errors

    def test_add_parameter_without_source(
        self,
        control_api: SyncHttpDatasetApiV1,
    ):
        ds = Dataset()
        ds_resp = control_api.apply_updates(
            dataset=ds,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "123",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        assert "Data source is not set for the dataset" in ds_resp.response_errors[0]
        assert ds_resp.dataset.find_field("test_title").id == "test_guid"

    def test_add_parameter_and_then_source(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        dataset_params: dict,
    ):
        ds = Dataset()
        ds_resp = control_api.apply_updates(
            dataset=ds,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "123",
                    },
                },
                {
                    "action": "add_source",
                    "source": {
                        "id": "source_1",
                        "title": "source_1",
                        "connection_id": saved_connection_id,
                        **dataset_params,
                    },
                },
                {
                    "action": "add_source_avatar",
                    "source_avatar": {
                        "id": "avatar_1",
                        "title": "avatar_1",
                        "source_id": "source_1",
                    },
                },
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK, ds_resp.response_errors

    def test_add_parameter_fails_on_invalid_default_value_type(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "abc",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST

    def test_add_parameter_fails_on_invalid_value_constraint(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "123",
                        "value_constraint": {
                            "type": "set",
                            "values": ["123", "abc"],
                        },
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST

    def test_add_parameter_fails_on_default_value_fails_constraint(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "789",
                        "value_constraint": {
                            "type": "set",
                            "values": ["123", "456"],
                        },
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST

        dataset = ds_resp.dataset

        field = dataset.find_field("test_title")
        assert field.valid is False
        field_error = ds_resp.dataset.component_errors.items[0]
        assert field_error.id == "test_guid"
        assert field_error.errors[0].code == "ERR.DS_API.FORMULA.PARAMETER.INVALID_VALUE"

    @pytest.mark.parametrize(
        "value_constraint",
        [
            None,
            {"type": "null"},
        ],
    )
    def test_add_template_str_parameter_fails_without_constraint(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        value_constraint: dict | None,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "test_title",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "string",
                        "default_value": "test",
                        "template_enabled": True,
                        "value_constraint": value_constraint,
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"

        ds = ds_resp.dataset
        field = ds.find_field("test_title")
        assert field.valid is False
        error = ds.component_errors.items[0].errors[0]
        assert error.code == "ERR.DS_API.SOURCE_CONFIG.PARAMETER_VALUE_CONSTRAINT_REQUIRED"

    def test_add_unknown_sys_parameter_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "_sys.unknown",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "string",
                        "default_value": "x",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"

        ds = ds_resp.dataset
        field = ds.find_field("_sys.unknown")
        assert field.valid is False
        error = ds.component_errors.items[0].errors[0]
        assert error.code == "ERR.DS_API.SOURCE_CONFIG.UNKNOWN_SYSTEM_PARAMETER"

    def test_add_known_sys_parameter_ok(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "_sys.user_id",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "string",
                        "default_value": "anon",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.OK, ds_resp.response_errors

    def test_add_non_parameter_with_sys_name_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        # The `_sys.` prefix is reserved for ALL field kinds, not only parameters.
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "_sys.user_id",
                        "calc_mode": dl_constants_enums.CalcMode.formula.value,
                        "formula": "'x'",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        ds = ds_resp.dataset
        field_errors = next(item for item in ds.component_errors.items if item.id == "test_guid")
        assert any(e.code == "ERR.DS_API.SOURCE_CONFIG.UNKNOWN_SYSTEM_PARAMETER" for e in field_errors.errors)

    def test_add_sys_user_id_wrong_type_fails(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ):
        # A registered system parameter must declare its expected type (string for _sys.user_id).
        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_guid",
                        "title": "_sys.user_id",
                        "calc_mode": dl_constants_enums.CalcMode.parameter.value,
                        "cast": "integer",
                        "default_value": "1",
                    },
                }
            ],
            fail_ok=True,
        )
        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        ds = ds_resp.dataset
        field_errors = next(item for item in ds.component_errors.items if item.id == "test_guid")
        assert any(e.code == "ERR.DS_API.SOURCE_CONFIG.SYSTEM_PARAMETER_TYPE_MISMATCH" for e in field_errors.errors)
