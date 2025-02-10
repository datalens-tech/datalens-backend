import http

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
