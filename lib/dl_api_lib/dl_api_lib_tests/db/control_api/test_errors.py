import json

import shortuuid

from dl_api_client.dsmaker.primitives import (
    CalcMode,
    Dataset,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.base_models import PathEntryLocation


class TestControlApiErrors(DefaultApiTestBase):
    def test_invalid_dataset_id(self, control_api, saved_connection_id, saved_dataset, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client
        path = PathEntryLocation(shortuuid.uuid())
        dash = us_client.create_entry(scope="dash", key=path)
        dash_id = dash["entryId"]

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id))
        assert resp.status_code == 200

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_connection_id))
        assert resp.status_code == 404

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(dash_id))
        assert resp.status_code == 404

    def test_create_entity_with_existing_name(self, control_api, saved_connection_id, saved_dataset, dataset_params):
        name = saved_dataset.name

        second_ds = Dataset(name=name)
        second_ds.sources["source_1"] = second_ds.source(
            connection_id=saved_connection_id,
            **dataset_params,
        )
        second_ds.source_avatars["avatar_1"] = second_ds.sources["source_1"].avatar()

        second_ds = control_api.apply_updates(dataset=second_ds).dataset
        resp = control_api.save_dataset(dataset=second_ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.json["message"] == "The entry already exists"
        assert resp.json["code"] == "ERR.DS_API.US.BAD_REQUEST.ALREADY_EXISTS"

    def test_create_connection_with_null_type(self, control_api, sync_us_manager):
        resp = control_api.client.post(
            "/api/v1/connections",
            data=json.dumps(
                {
                    "type": None,
                    "name": shortuuid.uuid(),
                }
            ),
            content_type="application/json",
        )

        assert resp.status_code == 400
        assert resp.json["message"] == "Invalid connection type value: None"
        assert resp.json["code"] == "ERR.DS_API.BAD_CONN_TYPE"

    def test_validate_formula_recursion_error(self, saved_dataset, control_api):
        # Create large code that will cause RecursionError
        num = 1000

        code = ""
        for if_num in range(num):
            code += f'if([row_id] = {if_num}, "row_id_{if_num}", '
        code += '"row_id_max"'
        code += ")" * num

        ds = saved_dataset
        title = "row_id to string"
        ds.result_schema[title] = ds.field(
            title=title,
            calc_mode=CalcMode.formula,
            formula=code,
        )

        resp = control_api.validate_field(dataset=saved_dataset, field=ds.find_field(title))
        assert resp.status_code == 400
        assert resp.json["message"] == "Validation finished with errors."
        assert resp.json["code"] == "ERR.DS_API.VALIDATION.ERROR"
        errors = resp.json["field_errors"][0]["errors"]
        assert len(errors) == 1
        assert errors[0]["message"] == "Failed to parse formula: maximum recursion depth exceeded"
        assert errors[0]["code"] == "FORMULA.PARSE.RECURSION"
