import http
import uuid

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants import FieldType


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
        assert resp.json["code"] == "OK"
        assert resp.json["message"] == "Validation was successful"
        assert resp.json["dataset"]["component_errors"] == {"items": []}
        fields_by_guid = {f["guid"]: f for f in resp.json["dataset"]["fields"]}
        assert fields_by_guid[new_field_id] == {
            "title": "Doubled",
            "guid": new_field_id,
            "data_type": "float",
            "calc_mode": "formula",
            "type": "DIMENSION",
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

    def test_valid_add_field_returns_resolved_field(
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
                    title="Now",
                    formula="NOW()",
                    type=FieldType.DIMENSION,
                ).add(),
            ],
            fields=[ds.field(id=new_field_id)],
        )

        assert resp.status_code == http.HTTPStatus.OK, resp.json
        fields_by_guid = {f["guid"]: f for f in resp.json["dataset"]["fields"]}
        new_field = fields_by_guid[new_field_id]
        assert new_field == {
            "title": "Now",
            "guid": new_field_id,
            "data_type": "genericdatetime",
            "calc_mode": "formula",
            "type": "DIMENSION",
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
        assert resp.json["code"] == "ERR.DS_API.VALIDATION.ERROR"
        assert resp.json["message"] == "Validation finished with errors."
        assert resp.json["dataset"]["component_errors"] == {
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
        }
        fields_by_guid = {f["guid"]: f for f in resp.json["dataset"]["fields"]}
        assert new_field_id in fields_by_guid
        assert fields_by_guid[new_field_id]["title"] == "Bad"
        assert fields_by_guid[new_field_id]["calc_mode"] == "formula"
