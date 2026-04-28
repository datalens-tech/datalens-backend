import http
import uuid

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import FieldType


class TestResultPreflight(DefaultApiTestBase):
    def test_valid_add_field_returns_ok(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        new_field_id = str(uuid.uuid4())

        resp = data_api.get_result_preflight(
            dataset=ds,
            updates=[
                ds.field(
                    id=new_field_id,
                    title="Doubled",
                    formula="[discount] * 2",
                    type=FieldType.MEASURE,
                ).add(),
            ],
            fields=[ds.field(id=new_field_id)],
        )

        assert resp.status_code == http.HTTPStatus.OK, resp.json
        assert resp.json == {
            "code": "OK",
            "message": "Validation was successful",
            "dataset": {"component_errors": {"items": []}},
        }

    def test_non_whitelisted_action_is_rejected(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        resp = data_api.get_result_preflight(
            dataset=ds,
            updates=[
                {
                    "action": "update_setting",
                    "setting": {
                        "name": "load_preview_by_default",
                        "value": True,
                    },
                },
            ],
            fields=[ds.find_field(title="discount")],
            fail_ok=True,
        )

        assert resp.status_code == http.HTTPStatus.BAD_REQUEST, resp.json
        assert resp.json == {
            "code": "ERR.DS_API.ACTION_NOT_ALLOWED",
            "message": "Action update_setting is not allowed.",
            "dataset": {"component_errors": {"items": []}},
        }

    def test_broken_formula_returns_field_component_error(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        new_field_id = str(uuid.uuid4())

        resp = data_api.get_result_preflight(
            dataset=ds,
            updates=[
                ds.field(
                    id=new_field_id,
                    title="Bad",
                    formula="NOT_A_FUNCTION([discount])",
                    type=FieldType.MEASURE,
                ).add(),
            ],
            fields=[ds.field(id=new_field_id)],
            fail_ok=True,
        )

        assert resp.status_code == http.HTTPStatus.BAD_REQUEST, resp.json
        assert resp.json == {
            "code": "ERR.DS_API.VALIDATION.ERROR",
            "message": "Validation finished with errors.",
            "dataset": {
                "component_errors": {
                    "items": [
                        {
                            "id": new_field_id,
                            "type": "field",
                            "errors": [
                                {
                                    "code": "ERR.DS_API.FORMULA.TRANSLATION.UNKNOWN_FUNCTION",
                                    "level": "error",
                                    "message": "Unknown 1-argument function or operator NOT_A_FUNCTION",
                                    "details": {
                                        "token": "not_a_function",
                                        "row": 0,
                                        "column": 0,
                                    },
                                },
                            ],
                        },
                    ],
                },
            },
        }
