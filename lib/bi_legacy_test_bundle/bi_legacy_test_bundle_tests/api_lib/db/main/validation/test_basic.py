from __future__ import annotations

from contextlib import contextmanager
from http import HTTPStatus
import json
from typing import (
    Dict,
    Set,
)
import uuid

import attr

from bi_legacy_test_bundle_tests.api_lib.utils import (
    data_source_settings_from_table,
    get_dataset_data,
    get_field_by_title,
    recycle_validation_response,
    validate_schema,
)
from dl_api_client.dsmaker.api.dataset_api import (
    HttpDatasetApiResponse,
    SyncHttpDatasetApiV1,
)
from dl_api_client.dsmaker.primitives import (
    Dataset,
    ObligatoryFilter,
    WhereClause,
)
from dl_api_lib.enums import WhereClauseOperation
from dl_connector_clickhouse.core.clickhouse.data_source import ClickHouseDataSource
from dl_constants.enums import (
    AggregationFunction,
    ComponentType,
    JoinType,
    TopLevelComponentId,
    UserDataType,
)
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_query_processing.exc import DatasetError


def validation_errors(response_data: dict) -> dict:
    return {key: value for key, value in response_data.items() if key.endswith("_errors") and value}


def get_error_code_from_source(ds: Dataset, source: str, error_index: int = 0) -> str:
    return ds.component_errors.get_pack(id=ds.sources[source].id).errors[error_index].code


def get_error_message_from_source(ds: Dataset, source: str, error_index: int = 0) -> str:
    return ds.component_errors.get_pack(id=ds.sources[source].id).errors[error_index].message


def get_avatars_error_code(ds: Dataset, source: str, error_index: int = 0) -> str:
    return ds.component_errors.get_pack(id=ds.source_avatars[source].id).errors[error_index].code


def test_dataset_updates(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    # Delete fields (all except for Profit)
    batch = [{"field": f, "action": "delete_field"} for f in result_schema if f["title"] != "Profit"]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 1
    assert result_schema[0]["title"] == "Profit"
    profit_guid = result_schema[0]["guid"]

    # Add a new field (a clone of Profit)
    new_field = result_schema[0].copy()
    new_field["title"] = "New Profit"
    del new_field["guid"]
    del new_field["cast"]
    del new_field["type"]
    del new_field["formula"]
    del new_field["guid_formula"]
    new_field = {k: v for k, v in new_field.items() if v is not None}
    ind = 1
    batch = [{"action": "add_field", "field": new_field, "order_index": ind}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "New Profit"
    assert result_schema[ind]["type"] == "DIMENSION"
    assert result_schema[ind]["cast"] == "float"

    field_guid = result_schema[ind]["guid"]

    # Update field:
    # - cast
    updated_field = {
        "guid": field_guid,
        "title": "Updated Profit",
        "cast": "string",
    }
    batch = [{"action": "update_field", "field": updated_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "Updated Profit"
    assert result_schema[ind]["cast"] == "string"

    # Update field:
    # - direct to formula
    # - dimension to measure
    # but with the same auto data type ==> cast should not change
    updated_field = {
        "guid": field_guid,
        "title": "Updated Profit 2",
        "calc_mode": "formula",
        "formula": "AVG([Profit])",
    }
    batch = [{"action": "update_field", "field": updated_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "Updated Profit 2"
    assert result_schema[ind]["calc_mode"] == "formula"
    assert result_schema[ind]["source"] == ""
    assert result_schema[ind]["formula"] == "AVG([Profit])"
    assert result_schema[ind]["guid_formula"] == f"AVG([{profit_guid}])"
    assert result_schema[ind]["type"] == "MEASURE"
    assert result_schema[ind]["cast"] == "string"

    # Update field:
    # - new auto data type ==> cast should be reset to default
    updated_field = {
        "guid": field_guid,
        "title": "Updated Profit 3",
        "formula": "INT(AVG([Profit]))",
    }
    batch = [{"action": "update_field", "field": updated_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "Updated Profit 3"
    assert result_schema[ind]["formula"] == "INT(AVG([Profit]))"
    assert result_schema[ind]["guid_formula"] == f"INT(AVG([{profit_guid}]))"
    assert result_schema[ind]["type"] == "MEASURE"
    assert result_schema[ind]["cast"] == "integer"

    # Update field:
    # - guid_formula instead of formula
    updated_field = {
        "guid": field_guid,
        "title": "Updated Profit 4",
        "guid_formula": f"AVG([{profit_guid}])",
    }
    batch = [{"action": "update_field", "field": updated_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "Updated Profit 4"
    assert result_schema[ind]["formula"] == "AVG([Profit])"
    assert result_schema[ind]["guid_formula"] == f"AVG([{profit_guid}])"
    assert result_schema[ind]["type"] == "MEASURE"
    assert result_schema[ind]["cast"] == "float"

    # Update field formula has priority over guid_formula
    updated_field = {
        "guid": field_guid,
        "title": "Updated Profit 4",
        "formula": "SUM([Profit])",
        "guid_formula": f"2 * SUM([{profit_guid}])",
    }
    batch = [{"action": "update_field", "field": updated_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 2
    assert result_schema[ind]["title"] == "Updated Profit 4"
    assert result_schema[ind]["formula"] == "SUM([Profit])"
    assert result_schema[ind]["guid_formula"] == f"SUM([{profit_guid}])"
    assert result_schema[ind]["type"] == "MEASURE"
    assert result_schema[ind]["cast"] == "float"

    # Add parameter field:
    new_field = result_schema[0].copy()
    new_field["title"] = "Parameterized Profit"
    del new_field["guid"]
    del new_field["type"]
    del new_field["source"]
    del new_field["formula"]
    del new_field["guid_formula"]

    new_field["calc_mode"] = "parameter"
    new_field["cast"] = "float"
    new_field["default_value"] = 100.5
    new_field["value_constraint"] = {"type": "range", "min": 0, "max": 9000.9}
    ind = 2
    batch = [{"action": "add_field", "field": new_field, "order_index": ind}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 3
    assert result_schema[ind]["title"] == "Parameterized Profit"
    assert result_schema[ind]["type"] == "DIMENSION"
    assert result_schema[ind]["cast"] == "float"
    assert result_schema[ind]["calc_mode"] == "parameter"
    assert result_schema[ind]["default_value"] == 100.5
    assert result_schema[ind]["value_constraint"]["type"] == "range"
    assert result_schema[ind]["value_constraint"]["min"] == 0
    assert result_schema[ind]["value_constraint"]["max"] == 9000.9

    # Add parameter field with incorrect type:
    new_field = result_schema[0].copy()
    new_field["title"] = "Incorrect parameterized Profit"
    del new_field["guid"]
    del new_field["type"]
    del new_field["source"]
    del new_field["formula"]
    del new_field["guid_formula"]
    new_field["calc_mode"] = "parameter"
    new_field["cast"] = "float"
    new_field["default_value"] = "foo"
    new_field["value_constraint"] = {
        "type": "all",
    }
    ind = 3
    batch = [{"action": "add_field", "field": new_field, "order_index": ind}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.BAD_REQUEST

    # Add parameter field with unsupported type:
    new_field = result_schema[0].copy()
    new_field["title"] = "Unsupported parameterized Profit"
    del new_field["guid"]
    del new_field["type"]
    del new_field["source"]
    del new_field["formula"]
    del new_field["guid_formula"]
    new_field["calc_mode"] = "parameter"
    new_field["cast"] = "uuid"
    new_field["default_value"] = "00000000-0000-0000-0000-000000000000"
    new_field["value_constraint"] = {
        "type": "all",
    }
    ind = 3
    batch = [{"action": "add_field", "field": new_field, "order_index": ind}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.BAD_REQUEST


def test_dataset_updates_add_field_with_error(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    # Add a new field (a clone of Profit)
    new_field = result_schema[0].copy()
    new_field["title"] = "New Invalid Field"
    del new_field["guid"]
    del new_field["cast"]
    del new_field["guid_formula"]
    new_field["type"] = "DIMENSION"
    new_field["calc_mode"] = "formula"
    new_field["formula"] = "AVG(STR([Profit]))"
    new_field = {k: v for k, v in new_field.items() if v is not None}
    batch = [{"action": "add_field", "field": new_field}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.BAD_REQUEST
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert result_schema[0]["title"] == "New Invalid Field"


def test_cascading_type_updates(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    # 1. Delete fields (all except for Profit)
    # 2. Make sure Profit is a 'float'
    batch = [
        *[
            {"action": "delete_field", "field": f} for f in result_schema if f["title"] != "Profit"
        ],  # delete all except Profit
        *[
            {"action": "update_field", "field": {"guid": f["guid"], "cast": "float", "title": "Profit 0"}}
            for f in result_schema
            if f["title"] == "Profit"
        ],  # update Profit
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    # Just make sure everything is OK
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == 1
    assert result_schema[0]["title"] == "Profit 0"

    # Add new fields as clones of Profit (via formula) with custom casts to 'integer'
    batch = []
    num = 3
    for i in range(num):
        new_field = result_schema[0].copy()
        del new_field["guid"]
        del new_field["type"]
        del new_field["guid_formula"]
        new_field["title"] = "Profit {}".format(i + 1)
        new_field["source"] = ""
        new_field["calc_mode"] = "formula"
        new_field["formula"] = "[Profit {}]".format(i)  # the previous clone or original
        new_field["cast"] = "integer"
        new_field = {k: v for k, v in new_field.items() if v is not None}
        batch.append({"action": "add_field", "field": new_field})
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    # Make sure they were all added correctly
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == num + 1  # new fields + the original one
    for i in range(num):
        ind = -i - 2
        field_data = result_schema[ind]
        assert field_data["title"] == "Profit {}".format(i + 1)
        assert field_data["formula"] == "[Profit {}]".format(i)
        assert field_data["type"] == "DIMENSION"
        assert field_data["cast"] == "integer"

    # Update original field's data type by changing its cast to 'string'
    original_guid = result_schema[-1]["guid"]
    batch = [{"action": "update_field", "field": {"guid": original_guid, "cast": "string"}}]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    # All of their casts should have been reset to 'string' in a cascading manner
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert len(result_schema) == num + 1  # new fields + the original one
    for i in range(num):
        assert result_schema[i]["cast"] == "string"


def test_rename_field_used_in_formula(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    field = next(filter(lambda f: f["cast"] == "float", result_schema))
    formula_title = "Doubled {}".format(field["title"])
    batch = [
        {
            "action": "add_field",
            "field": {
                "title": "Doubled {}".format(field["title"]),
                "calc_mode": "formula",
                "formula": "[{}] * 2".format(field["title"]),
            },
        }
    ]

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json

    new_title = "Renamed {}".format(field["title"])
    batch = [
        {
            "action": "update_field",
            "field": {
                "guid": field["guid"],
                "title": new_title,
            },
        }
    ]

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]

    formula_field = get_field_by_title(result_schema, title=formula_title)
    assert "[{}]".format(new_title) in formula_field["formula"]


def test_with_incoming_null_values(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    for field in result_schema:
        field.update({"valid": None, "data_type": None})

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data)
    assert response.status_code == HTTPStatus.OK


def test_with_absent_parameter_values_for_non_parameter_fields(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    for field in result_schema:
        if field["calc_mode"] != "parameter":
            field.pop("default_value", None)
            field.pop("value_constraint", None)

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data)
    assert response.status_code == HTTPStatus.OK


def test_default_type_string_for_invalid_fields(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    added_field = result_schema[-1].copy()
    del added_field["formula"]
    del added_field["guid_formula"]
    added_field["guid"] = str(uuid.uuid4())
    added_field["source"] = "Nonexistent Field"
    del added_field["cast"]
    del added_field["data_type"]
    added_field = {k: v for k, v in added_field.items() if v is not None}
    batch = [
        {
            "action": "add_field",
            "field": added_field,
        }
    ]

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.BAD_REQUEST
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]
    assert result_schema[0]["cast"] == "string"
    assert result_schema[0]["data_type"] == "string"


def test_formula_is_broken_and_fixed_again(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]

    float_title = next(filter(lambda f: f["cast"] == "float", result_schema))["title"]
    formula_title = "Doubled {}".format(float_title)
    batch = [
        {
            "action": "add_field",
            "field": {
                "title": "Doubled {}".format(float_title),
                "calc_mode": "formula",
                "formula": "[{}] * 2".format(float_title),
            },
        }
    ]

    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json
    result_schema = dataset_data["dataset"]["result_schema"]

    f_field = next(filter(lambda f: f["title"] == formula_title, result_schema))
    batch = [
        {
            "action": "update_field",
            "field": {
                "guid": f_field["guid"],
                "formula": "GREATEST([{}] * 2, ".format(float_title),
            },
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.BAD_REQUEST

    batch = [
        {
            "action": "update_field",
            "field": {
                "guid": f_field["guid"],
                "formula": "GREATEST([{}] * 2, 8)".format(float_title),
            },
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)
    assert response.status_code == HTTPStatus.OK


def test_structural_errors(client, static_dataset_id):
    dataset_id = static_dataset_id
    dataset_data = get_dataset_data(client, dataset_id)
    result_schema = dataset_data["dataset"]["result_schema"]
    old_field = result_schema[0]
    another_old_field = result_schema[1]

    # Delete nonexistent field
    batch = [
        {
            "action": "delete_field",
            "field": {"guid": str(uuid.uuid4())},
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK  # FIXME
    # assert response.json['code'] == 'ERR.DS_API.VALIDATION.ERROR'

    # Add field with existing ID
    batch = [
        {
            "action": "add_field",
            "field": {"guid": old_field["guid"], "title": "New Field"},
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK  # FIXME
    # assert response.json['code'] == 'ERR.DS_API.VALIDATION.FATAL'

    # Add field with existing title
    batch = [
        {
            "action": "add_field",
            "field": {"guid": str(uuid.uuid4()), "title": old_field["title"], "calc_mode": "formula", "formula": "100"},
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    def get_field_errors(_response) -> Dict[str, dict]:
        return {
            err_pack["id"]: err_pack
            for err_pack in _response.json["dataset"]["component_errors"]["items"]
            if err_pack["type"] == "field"
        }

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert len(get_field_errors(response)) == 2  # for both fields with the same title
    assert set(get_field_errors(response)) == {batch[0]["field"]["guid"], old_field["guid"]}
    assert len(response.json["dataset"]["result_schema"]) == len(result_schema) + 1

    # Update field with nonexistent ID
    batch = [
        {
            "action": "update_field",
            "field": {"guid": str(uuid.uuid4()), "title": "New Title"},
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.OK
    # assert response.json['code'] == 'ERR.DS_API.VALIDATION.FATAL'

    # Update field with existing title
    batch = [
        {
            "action": "update_field",
            "field": {"guid": old_field["guid"], "title": another_old_field["title"]},
        }
    ]
    response = validate_schema(client, dataset_id=dataset_id, dataset_data=dataset_data, update_batch=batch)

    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert len(get_field_errors(response)) == 2
    assert set(get_field_errors(response)) == {batch[0]["field"]["guid"], another_old_field["guid"]}


def test_dataset_validate_schema(client, api_v1, ch_data_source_settings):
    ds = Dataset()

    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    f0 = ds.result_schema[0]

    # valid field
    ds.result_schema["f1"] = ds.field(formula=f"MAX([{f0.title}])")
    # 2 translation errors
    ds.result_schema["f2"] = ds.field(formula=f"UNKNOWN_FUNC([{f0.title}]) + ANOTHER_UNKNOWN_FUNC([{f0.title}])")
    col_0, col_1 = 0, ds.result_schema["f2"].formula.find("ANOTHER")
    # parse error
    ds.result_schema["f3"] = ds.field(formula="SYNTAX ERROR")

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset

    errors_by_title = {
        ds.find_field(id=err_pack["id"]).title: err_pack
        for err_pack in ds_resp.json["dataset"]["component_errors"]["items"]
        if err_pack["type"] == "field"
    }
    assert set(errors_by_title) == {"f2", "f3"}
    assert len(errors_by_title["f2"]["errors"]) == 2
    assert errors_by_title["f2"]["errors"][0]["details"]["column"] == col_0  # positions of invalid functions
    assert errors_by_title["f2"]["errors"][1]["details"]["column"] == col_1
    assert len(errors_by_title["f3"]["errors"]) == 1

    assert ds.result_schema[-1]  # f0
    assert ds.result_schema["f1"].valid
    assert not ds.result_schema["f2"].valid
    assert not ds.result_schema["f3"].valid


def test_dynamic_dataset_no_actions(client, static_dataset_id):
    # empty dataset
    response = client.post("/api/v1/datasets/validators/dataset", data=json.dumps({}), content_type="application/json")
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json["dataset"]
    assert dataset_data["result_schema"] == []
    assert dataset_data["sources"] == []
    assert dataset_data["source_avatars"] == []
    assert dataset_data["avatar_relations"] == []
    assert dataset_data["component_errors"]["items"] == []

    # dataset with source
    dataset_data = client.get("/api/v1/datasets/{}/versions/draft".format(static_dataset_id)).json["dataset"]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        data=json.dumps(
            dict(
                dataset=dict(
                    sources=dataset_data["sources"],
                    source_avatars=dataset_data["source_avatars"],
                    avatar_relations=dataset_data["avatar_relations"],
                    result_schema=dataset_data["result_schema"],
                ),
            )
        ),
        content_type="application/json",
    )
    assert response.status_code == HTTPStatus.OK
    updated_dataset_data = response.json["dataset"]
    assert updated_dataset_data["result_schema"] == dataset_data["result_schema"]
    assert updated_dataset_data["sources"] == dataset_data["sources"]
    assert updated_dataset_data["source_avatars"] == dataset_data["source_avatars"]
    assert updated_dataset_data["avatar_relations"] == dataset_data["avatar_relations"]
    assert updated_dataset_data["component_errors"] == dataset_data["component_errors"]


def test_source_actions(client, api_v1, ch_data_source_settings, ch_other_data_source_settings):
    ds = Dataset()
    ds = api_v1.apply_updates(dataset=ds).dataset
    data = api_v1.dump_dataset_to_request_body(dataset=ds)

    # add_source
    source = dict(ch_data_source_settings, id=str(uuid.uuid4()), title="Source 1")
    data["updates"] = [{"action": "add_source", "source": source}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json["dataset"]
    assert dataset_data["result_schema"] == []
    assert len(dataset_data["sources"]) == 1
    assert len(dataset_data["sources"][0]["raw_schema"]) > 0
    assert dataset_data["sources"][0]["title"] == "Source 1"
    assert dataset_data["source_avatars"] == []
    assert dataset_data["avatar_relations"] == []
    assert dataset_data["component_errors"]["items"] == []

    # update_source
    data = recycle_validation_response(response.json)
    source = dict(ch_other_data_source_settings, id=source["id"], title="Renamed Source")
    data["updates"] = [{"action": "update_source", "source": source}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK, validation_errors(response.json)
    dataset_data = response.json["dataset"]
    assert dataset_data["result_schema"] == []
    assert len(dataset_data["sources"]) == 1
    assert len(dataset_data["sources"][0]["raw_schema"]) > 0
    assert dataset_data["sources"][0]["title"] == "Renamed Source"
    assert dataset_data["sources"][0]["parameters"]["table_name"] == (
        ch_other_data_source_settings["parameters"]["table_name"]
    )

    # refresh_source
    old_raw_schema_len = len(response.json["dataset"]["sources"][0]["raw_schema"])
    data = recycle_validation_response(response.json)
    del data["dataset"]["sources"][0]["raw_schema"][-1]
    source = dict(ch_other_data_source_settings, id=source["id"], title="Renamed Source")
    data["updates"] = [{"action": "refresh_source", "source": data["dataset"]["sources"][0]}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert len(response.json["dataset"]["sources"][0]["raw_schema"]) == old_raw_schema_len
    assert response.status_code == HTTPStatus.OK

    # delete_source
    data = recycle_validation_response(response.json)
    data["updates"] = [{"action": "delete_source", "source": {"id": source["id"]}}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json["dataset"]
    assert dataset_data["result_schema"] == []
    assert dataset_data["sources"] == []


def test_sources_same_title_different_db(client, api_v1, ch_data_source_settings, ch_data_source_settings_for_other_db):
    ds = Dataset()
    ds = api_v1.apply_updates(dataset=ds).dataset
    data = api_v1.dump_dataset_to_request_body(dataset=ds)

    # add_source
    source = dict(ch_data_source_settings, id=str(uuid.uuid4()), title="test_table")
    data["updates"] = [{"action": "add_source", "source": source}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json["dataset"]
    assert len(dataset_data["sources"]) == 1
    assert dataset_data["sources"][0]["title"] == "test_table"
    assert dataset_data["sources"][0]["parameters"]["db_name"] == (ch_data_source_settings["parameters"]["db_name"])

    # add a source from a table of the same name, but from a different db,
    # then remove the first source; no errors should arise even with the same titles,
    # because conflicts will be resolved by the time all the updates would be applied
    data = recycle_validation_response(response.json)
    other_source = dict(ch_data_source_settings_for_other_db, id=str(uuid.uuid4()), title="test_table")
    data["updates"] = [
        {"action": "add_source", "source": other_source},
        {"action": "delete_source", "source": {"id": source["id"]}},
    ]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    dataset_data = response.json["dataset"]
    assert len(dataset_data["sources"]) == 1
    assert dataset_data["sources"][0]["title"] == "test_table"
    assert dataset_data["sources"][0]["parameters"]["db_name"] == (
        ch_data_source_settings_for_other_db["parameters"]["db_name"]
    )


def test_source_avatar_actions(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds = api_v1.apply_updates(dataset=ds).dataset

    # add_source_avatar
    ds.source_avatars["Avatar"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds_resp.json["dataset"]["source_avatars"] == [
        dict(
            id=ds.source_avatars["Avatar"].id,
            title="Avatar",
            source_id=ds.sources["source_1"].id,
            is_root=True,
            managed_by="user",
            virtual=False,
            valid=True,
        )
    ]
    assert len(ds_resp.json["dataset"]["result_schema"]) > 0
    assert ds_resp.json["dataset"]["result_schema"][0]["avatar_id"] == ds.source_avatars["Avatar"].id

    # update_source_avatar
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.source_avatars["Avatar"].update(title="Renamed Avatar")])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds_resp.json["dataset"]["source_avatars"] == [
        dict(
            id=ds.source_avatars["Avatar"].id,
            title="Renamed Avatar",
            source_id=ds.sources["source_1"].id,
            is_root=True,
            managed_by="user",
            virtual=False,
            valid=True,
        )
    ]

    # delete_source_avatar
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.source_avatars["Avatar"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    assert ds_resp.json["dataset"]["source_avatars"] == []
    assert ds_resp.json["dataset"]["result_schema"] == []


def test_avatar_relation_actions(client, api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    data = api_v1.dump_dataset_to_request_body(dataset=ds)

    # add_avatar_relation
    relation = {
        "id": str(uuid.uuid4()),
        "left_avatar_id": ds.source_avatars["avatar_1"].id,
        "right_avatar_id": ds.source_avatars["avatar_2"].id,
        "conditions": [
            {
                "type": "binary",
                "operator": "eq",
                "left": {"calc_mode": "direct", "source": "Order ID"},
                "right": {"calc_mode": "direct", "source": "Order ID"},
            },
        ],
        "join_type": "inner",
    }
    data["updates"] = [{"action": "add_avatar_relation", "avatar_relation": relation}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json["dataset"]["avatar_relations"] == [dict(relation, managed_by="user", virtual=False)]

    data = recycle_validation_response(response.json)
    data["updates"] = [
        {"action": "update_avatar_relation", "avatar_relation": {"id": relation["id"], "join_type": "left"}}
    ]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    assert response.json["dataset"]["avatar_relations"][0]["join_type"] == "left"

    data = recycle_validation_response(response.json)
    data["updates"] = [{"action": "delete_avatar_relation", "avatar_relation": {"id": relation["id"]}}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    assert len(response.json["dataset"]["avatar_relations"]) == 0

    # add_avatar_relation (no conditions) -> auto condition
    data = recycle_validation_response(response.json)
    relation = {
        "id": str(uuid.uuid4()),
        "left_avatar_id": ds.source_avatars["avatar_1"].id,
        "right_avatar_id": ds.source_avatars["avatar_2"].id,
        "conditions": [],
        "join_type": "inner",
    }
    data["updates"] = [{"action": "add_avatar_relation", "avatar_relation": relation}]
    response = client.post(
        "/api/v1/datasets/validators/dataset",
        content_type="application/json",
        data=json.dumps(data),
    )
    assert response.status_code == HTTPStatus.OK
    assert len(response.json["dataset"]["avatar_relations"][0]["conditions"]) == 1
    condition = response.json["dataset"]["avatar_relations"][0]["conditions"][0]
    assert condition["left"]["source"] == condition["right"]["source"] == "Category"


def test_create_edit_and_save_multisource(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds.avatar_relations["relation_1"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds = api_v1.load_dataset(dataset=ds).dataset
    assert len(ds.sources) == 1
    assert len(ds.source_avatars) == 2
    assert ds.source_avatars["avatar_1"].title == "avatar_1"
    assert ds.source_avatars["avatar_2"].title == "avatar_2"
    assert ds.source_avatars["avatar_1"].is_root
    assert not ds.source_avatars["avatar_2"].is_root
    assert len(ds.avatar_relations) == 1

    ds = api_v1.apply_updates(dataset=ds, updates=[ds.source_avatars["avatar_2"].update(title="Renamed")]).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds = api_v1.load_dataset(dataset=ds).dataset
    assert len(ds.source_avatars) == 2
    assert ds.source_avatars["avatar_1"].title == "avatar_1"
    assert ds.source_avatars["avatar_2"].title == "Renamed"

    ds = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.avatar_relations["relation_1"].delete(),
            ds.source_avatars["avatar_1"].delete(),
        ],
    ).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds = api_v1.load_dataset(dataset=ds).dataset
    assert len(ds.source_avatars) == 1
    assert ds.source_avatars["avatar_2"].is_root


def test_field_actions_multisource(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds.avatar_relations["relation_1"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds = api_v1.apply_updates(dataset=ds).dataset

    # valid source
    ds.result_schema["field_1"] = ds.source_avatars["avatar_1"].field(source="Order ID")
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    # valid formula
    ds.result_schema["field_2"] = ds.field(formula='CONCAT("#", [Order ID], [Order ID (1)])')
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    # invalid source
    ds.result_schema["field_3"] = ds.source_avatars["avatar_1"].field(source="My Order ID")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.result_schema["field_3"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    # invalid formula (unknown field)
    ds.result_schema["field_4"] = ds.field(formula='CONCAT("#", [Order ])')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.result_schema["field_4"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK


def test_validate_field_dynamic_ds(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds.avatar_relations["relation_1"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds = api_v1.apply_updates(dataset=ds).dataset

    # valid formula
    resp = api_v1.validate_field(
        dataset=ds, field=ds.field(title="New field", formula='CONCAT("#", [Order ID], [Order ID (1)])')
    )
    assert resp.status_code == HTTPStatus.OK

    # invalid formula (unknown field)
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula='CONCAT("#", [Order ])'))
    assert resp.status_code == HTTPStatus.BAD_REQUEST
    # invalid formula (invalid syntax)
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula='CONCAT("#", [whaaa'))
    assert resp.status_code == HTTPStatus.BAD_REQUEST


def test_validate_field_saved_ds(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    first_field = ds.result_schema[0]

    # valid formula
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula="1 + 1"))
    assert resp.status_code == HTTPStatus.OK

    unknown_func_formula = "UNKNOWN_FUNC([{0}]) + ANOTHER_UNKNOWN_FUNC([{0}])".format(first_field.title)
    col_0, col_1 = 0, unknown_func_formula.find("ANOTHER")
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=unknown_func_formula))
    assert resp.status_code == HTTPStatus.BAD_REQUEST
    errors = resp.json["field_errors"][0]["errors"]
    assert len(errors) == 2
    assert errors[0]["column"] == col_0  # positions of invalid functions
    assert errors[1]["column"] == col_1

    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula="SYNTAX ERROR"))
    assert resp.status_code == HTTPStatus.BAD_REQUEST
    errors = resp.json["field_errors"][0]["errors"]
    assert len(errors) == 1


def test_validate_field_empty_formula(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    # Really empty
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=""))
    assert resp.status_code == HTTPStatus.OK

    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=" "))
    assert resp.status_code == HTTPStatus.OK

    # Comments
    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=" --[value]"))
    assert resp.status_code == HTTPStatus.OK

    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=" -- something"))
    assert resp.status_code == HTTPStatus.OK

    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="New field", formula=" /* something */\n --else"))
    assert resp.status_code == HTTPStatus.OK


@contextmanager
def hacked_join_types(join_types: Set[JoinType]):
    old_join_types = ClickHouseDataSource.supported_join_types
    try:
        ClickHouseDataSource.supported_join_types = join_types
        yield
    finally:
        ClickHouseDataSource.supported_join_types = old_join_types


def test_relation_errors(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    # mismatching data types
    ds.avatar_relations["relation_1"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order Date"))
    )
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 1
    assert len(ds.component_errors.get_pack(id=ds.avatar_relations["relation_1"].id).errors) > 0

    # remove invalid relation
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.avatar_relations["relation_1"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0

    # mismatching data types
    ds.avatar_relations["relation_2"] = (
        ds.source_avatars["avatar_1"]
        .join(ds.source_avatars["avatar_2"])
        .on(ds.col("Order ID") == ds.col("Nonexistent Field"))
    )
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.get_pack(id=ds.avatar_relations["relation_2"].id).errors) > 0

    # remove invalid relation
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.avatar_relations["relation_2"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0

    # valid relation
    ds.avatar_relations["relation_3"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    with hacked_join_types(join_types={JoinType.inner}):
        ds_resp = api_v1.apply_updates(
            dataset=ds, updates=[ds.avatar_relations["relation_3"].update(join_type="left")], fail_ok=True
        )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.get_pack(id=ds.avatar_relations["relation_3"].id).errors) > 0


def _chnt(name):
    """DSMaker-protocol CH NativeType maker"""
    return {
        "conn_type": "clickhouse",
        "native_type_class_name": "clickhouse_native_type",
        "name": name.lower(),
        "lowcardinality": False,
        "nullable": False,
    }


def test_resolve_old_fields_for_new_source_in_avatar(api_v1, ch_data_source_settings):
    ds = Dataset()
    new_dsrc_settings = dict(
        ch_data_source_settings, parameters=dict(ch_data_source_settings["parameters"], table_name="new_table")
    )

    ds.sources["old_source"] = ds.source(**ch_data_source_settings, created_=True)  # bypass generation of raw_schema
    ds.sources["new_source"] = ds.source(**new_dsrc_settings, created_=True)
    ds.sources["old_source"].raw_schema = [
        ds.col(name="first_col", title="First Col", user_type=UserDataType.string, native_type=_chnt(name="string")),
        ds.col(name="second_col", title="Second Col", user_type=UserDataType.string, native_type=_chnt(name="string")),
    ]
    ds.sources["new_source"].raw_schema = [
        ds.col(name="new_second", title="Second Col", user_type=UserDataType.string, native_type=_chnt(name="string")),
        ds.col(name="new_first", title="First Col", user_type=UserDataType.string, native_type=_chnt(name="string")),
    ]
    ds.source_avatars["avatar"] = ds.sources["old_source"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    assert [f.title for f in ds.result_schema] == ["First Col", "Second Col"]
    ds = api_v1.apply_updates(
        dataset=ds, updates=[ds.source_avatars["avatar"].update(source_id=ds.sources["new_source"].id)]
    ).dataset
    assert ds.source_avatars["avatar"].source_id == ds.sources["new_source"].id
    assert [f.title for f in ds.result_schema] == ["First Col", "Second Col"]
    assert [f.source for f in ds.result_schema] == ["new_first", "new_second"]


def test_resolve_old_fields_for_updated_old_source(api_v1, ch_data_source_settings):
    ds = Dataset()
    old_dsrc_settings = dict(
        ch_data_source_settings, parameters=dict(ch_data_source_settings["parameters"], table_name="my_table")
    )
    ds.sources["source"] = ds.source(**old_dsrc_settings, created_=True)  # bypass generation of raw_schema
    ds.sources["source"].raw_schema = [
        ds.col(name="first_col", title="Order ID", user_type=UserDataType.string, native_type=_chnt(name="string")),
        ds.col(name="second_col", title="Category", user_type=UserDataType.string, native_type=_chnt(name="string")),
        ds.col(name="third_col", title="Unknown", user_type=UserDataType.string, native_type=_chnt(name="string")),
    ]
    ds.source_avatars["avatar"] = ds.sources["source"].avatar()
    ds.result_schema["My Formula"] = ds.field(formula="[third_col]")
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert [f.title for f in ds.result_schema] == ["My Formula", "Order ID", "Category", "Unknown"]
    assert [f.source for f in ds.result_schema] == ["", "first_col", "second_col", "third_col"]

    ds = api_v1.apply_updates(
        dataset=ds,
        updates=[ds.sources["source"].update(parameters=ch_data_source_settings["parameters"])],
        fail_ok=True,
    ).dataset

    # third field should be not be deleted
    # check the first 4 fields only, the rest should be filled with fields from the real table's raw_schema
    assert [f.title for f in ds.result_schema][:4] == ["My Formula", "Order ID", "Category", "Unknown"]
    assert [f.source for f in ds.result_schema][:4] == ["", "Order ID", "Category", "third_col"]
    assert [f.formula for f in ds.result_schema][:4] == ["[third_col]", "", "", ""]
    assert len(ds.result_schema) > 4


def test_source_deduplication(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.sources["source_2"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_2"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    assert len(ds.sources) == 1
    assert ds.source_avatars["avatar_1"].source_id == ds.sources["source_1"].id
    assert ds.source_avatars["avatar_2"].source_id == ds.sources["source_1"].id


def test_validation_after_saving(api_v1, ch_data_source_settings, ch_other_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds.sources["source_2"] = ds.source(**ch_other_data_source_settings)
    ds.source_avatars["avatar_2"] = ds.sources["source_2"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK


def test_deletion_of_avatars_for_saved_dataset(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_3"] = ds.sources["source_1"].avatar()
    ds.avatar_relations["relation_1"] = (
        ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds.avatar_relations["relation_2"] = (
        ds.source_avatars["avatar_2"].join(ds.source_avatars["avatar_3"]).on(ds.col("Order ID") == ds.col("Order ID"))
    )
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.avatar_relations["relation_2"].delete(),
            ds.source_avatars["avatar_3"].delete(),
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.avatar_relations["relation_1"].delete(),
            ds.source_avatars["avatar_2"].delete(),
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK


def test_adding_field_with_nonexistent_avatar(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    ds.result_schema["new field"] = ds.field(source="some_field", avatar_id="idontexist")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST


def test_type_autoupdate_for_multiple_fields(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds.result_schema["my_date"] = ds.field(formula="#2019-04-05#")
    ds.result_schema["my_date_dependent_1"] = ds.field(formula="[my_date]")
    ds.result_schema["my_date_dependent_2"] = ds.field(formula="[my_date]")
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds = api_v1.apply_updates(dataset=ds, updates=[ds.result_schema["my_date"].update(cast="datetime")]).dataset
    assert ds.result_schema["my_date_dependent_1"].cast == UserDataType.genericdatetime
    assert ds.result_schema["my_date_dependent_2"].cast == UserDataType.genericdatetime


def test_formula_recursion(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    resp = api_v1.validate_field(dataset=ds, field=ds.field(title="new field", formula="[new field]"))
    assert resp.status_code == HTTPStatus.BAD_REQUEST

    ds.result_schema["new field"] = ds.field(formula="[new field]")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert not ds.result_schema["new field"].valid


def test_substitution_of_formula_in_formula(data_api_v1, api_v1, ch_data_source_settings, default_sync_usm):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    orig_field = ds.result_schema[0]
    ds.result_schema["First Field"] = ds.field(formula=f"BOOL([{orig_field.title}])")
    ds.result_schema["Second Field"] = ds.field(formula="STR([First Field])")
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds = api_v1.load_dataset(dataset=ds).dataset

    result_resp = data_api_v1.get_result(
        dataset=ds,
        fields=[
            ds.result_schema["First Field"],
            ds.result_schema["Second Field"],
        ],
        limit=2,
    )
    assert result_resp.status_code == HTTPStatus.OK


def test_avatar_without_fields(api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[field.delete() for field in ds.result_schema if field.avatar_id == ds.source_avatars["avatar_2"].id],
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.source_avatars["avatar_2"].delete()])
    assert ds_resp.status_code == HTTPStatus.OK


def test_source_errors(api_v1, ch_data_source_settings, default_sync_usm):
    ds = Dataset()
    # add an invalid data source
    ds.sources["source_1"] = ds.source(
        **dict(
            ch_data_source_settings,
            parameters=dict(ch_data_source_settings["parameters"], table_name="The Round Table"),
        )
    )
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.sources) == 1, "Source was not added"
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 1
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"
    assert "The Round Table" in get_error_message_from_source(ds, "source_1")

    # make it valid via update_source
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[ds.sources["source_1"].update(parameters=dict(ch_data_source_settings["parameters"]))],
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 0

    # make it invalid again via update_source
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.sources["source_1"].update(
                parameters=dict(ch_data_source_settings["parameters"], table_name="The Round Table")
            )
        ],
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 1
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"
    assert "The Round Table" in get_error_message_from_source(ds, "source_1")

    ds = api_v1.save_dataset(dataset=ds).dataset
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 1
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"
    assert "The Round Table" in get_error_message_from_source(ds, "source_1")

    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 1
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"
    assert "The Round Table" in get_error_message_from_source(ds, "source_1")

    ds.component_errors.items.clear()  # remove info about errors
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK  # errors are no longer generated from the validity status
    ds = ds_resp.dataset
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 0

    ds = api_v1.apply_updates(dataset=ds, updates=[ds.sources["source_1"].refresh()], fail_ok=True).dataset
    assert not ds.sources["source_1"].valid
    assert len(ds.component_errors.items) == 1
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"
    assert "The Round Table" in get_error_message_from_source(ds, "source_1")


def test_source_title_errors(api_v1, ch_data_source_settings, ch_other_data_source_settings, default_sync_usm):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings, title="Source")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds.sources["source_2"] = ds.source(**ch_other_data_source_settings, title="Source")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 2
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"
    assert get_error_code_from_source(ds, "source_2") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"

    ds = api_v1.save_dataset(dataset=ds).dataset
    assert len(ds.component_errors.items) == 2
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"
    assert get_error_code_from_source(ds, "source_2") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"

    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert len(ds.component_errors.items) == 2
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"
    assert get_error_code_from_source(ds, "source_1") == "ERR.DS_API.SOURCE.TITLE.CONFLICT"

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.sources["source_1"].update(title="Renamed")], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0


def test_avatar_title_errors(api_v1, ch_data_source_settings, ch_other_data_source_settings, default_sync_usm):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings, title="Source")
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar(title="Avatar")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    ds.source_avatars["avatar_2"] = ds.sources["source_1"].avatar(title="Avatar")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 2
    assert get_avatars_error_code(ds, "avatar_1") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"
    assert get_avatars_error_code(ds, "avatar_2") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"

    ds = api_v1.save_dataset(dataset=ds).dataset
    assert len(ds.component_errors.items) == 2
    assert get_avatars_error_code(ds, "avatar_1") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"
    assert get_avatars_error_code(ds, "avatar_2") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"

    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert len(ds.component_errors.items) == 2
    assert get_avatars_error_code(ds, "avatar_1") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"
    assert get_avatars_error_code(ds, "avatar_2") == "ERR.DS_API.AVATAR.TITLE.CONFLICT"

    ds_resp = api_v1.apply_updates(
        dataset=ds, updates=[ds.source_avatars["avatar_1"].update(title="Renamed")], fail_ok=True
    )
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0


def test_obligatory_filter_actions(api_v1: SyncHttpDatasetApiV1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds_resp.json["dataset"]["obligatory_filters"] == []

    category_field = ds.result_schema[0]
    city_field = ds.result_schema[1]

    # add obligatory filter
    filter_id = str(uuid.uuid4())
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ObligatoryFilter(
                id=filter_id,
                field_guid=category_field.id,
                default_filters=[
                    WhereClause(
                        column=category_field.id, operation=WhereClauseOperation.EQ, values=["Office Supplies"]
                    ),
                ],
            ).add()
        ],
    )

    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds_resp.json["dataset"]["obligatory_filters"]) == 1
    assert len(ds_resp.json["dataset"]["obligatory_filters"][0]["default_filters"]) == 1
    assert ds_resp.json["dataset"]["obligatory_filters"][0]["id"] == filter_id
    assert ds_resp.json["dataset"]["obligatory_filters"][0]["field_guid"] == category_field.id
    assert ds_resp.json["dataset"]["obligatory_filters"][0]["managed_by"] == "user"
    assert ds_resp.json["dataset"]["obligatory_filters"][0]["valid"]

    # update obligatory filter
    obligatory_filter = ds.obligatory_filters[0]
    obligatory_filter.default_filters = [
        WhereClause(
            column=category_field.id,
            operation=WhereClauseOperation.IN,
            values=["Office Supplies", "Furniture"],
        ),
    ]
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[obligatory_filter.update()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds_resp.json["dataset"]["obligatory_filters"]) == 1
    assert len(ds_resp.json["dataset"]["obligatory_filters"][0]["default_filters"]) == 1
    assert len(ds_resp.json["dataset"]["obligatory_filters"][0]["default_filters"][0]["values"]) == 2
    assert ds_resp.json["dataset"]["obligatory_filters"][0]["valid"]

    # add filter for one more field
    filter_id = str(uuid.uuid4())
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ObligatoryFilter(
                id=filter_id,
                field_guid=city_field.id,
                default_filters=[
                    WhereClause(column=city_field.id, operation=WhereClauseOperation.EQ, values=["Houston"]),
                ],
            ).add()
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds_resp.json["dataset"]["obligatory_filters"]) == 2

    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.obligatory_filters) == 2

    # delete first obligatory filter
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.obligatory_filters[0].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds_resp.json["dataset"]["obligatory_filters"]) == 1

    # delete last obligatory filter
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.obligatory_filters[0].delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    assert ds_resp.json["dataset"]["obligatory_filters"] == []


def test_obligatory_filter_change_field(api_v1: SyncHttpDatasetApiV1, ch_data_source_settings):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(**ch_data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    category_field = ds.result_schema[0]

    # add obligatory filter
    filter_id = str(uuid.uuid4())
    ds = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ObligatoryFilter(
                id=filter_id,
                field_guid=category_field.id,
                default_filters=[
                    WhereClause(
                        column=category_field.id, operation=WhereClauseOperation.STARTSWITH, values=["Furnitu"]
                    ),
                ],
            ).add()
        ],
    ).dataset

    # try to delete field before obligatory filter
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.result_schema[0].delete()], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST

    # try to change field type to type with incompatible filter type
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.result_schema[0].update(cast="date")], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 1
    assert ds.component_errors.get_pack(id=filter_id).errors[0].code == "ERR.DS_API.DS_CONFIG"
    assert ds.obligatory_filters[0].id == filter_id
    assert not ds.obligatory_filters[0].valid


def test_field_is_valid_fix_on_refresh_source(api_v1: SyncHttpDatasetApiV1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    for field in ds.result_schema:
        assert field.valid

    ds.result_schema[0].valid = False
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert not ds.result_schema[0].valid

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.sources[0].refresh(force_update_fields=True)], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    for field in ds.result_schema:
        assert field.valid


def test_field_is_invalid_after_refresh_source_with_deleted_column(
    api_v1: SyncHttpDatasetApiV1, clickhouse_db, static_connection_id
):
    db = clickhouse_db
    connection_id = static_connection_id

    table_name = str(uuid.uuid4())
    table = make_table(
        db=db,
        name=table_name,
        columns=[
            C.int_value(name="old_int"),
            C.datetime_value(name="old_datetime"),
        ],
    )

    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(table))
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds.result_schema["formula_1"] = ds.field(formula="[old_int] + 1")
    ds.result_schema["formula_2"] = ds.field(formula="[formula_1] + 1")
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 4
    for field in ds.result_schema:
        assert field.valid

    # change table's raw schema (remove old columns and make new ones)
    db.drop_table(table.table)
    make_table(
        db=db,
        name=table_name,
        columns=[
            C.int_value(name="new_int"),
            C.datetime_value(name="new_datetime"),
        ],
    )

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.sources["source_1"].refresh()], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 6
    fields = [f for f in ds.result_schema]
    field_infos = [(f.title, f.valid) for f in fields]
    expected_field_infos = [
        ("formula_2", False),
        ("formula_1", False),
        ("old_int", False),
        ("old_datetime", False),
        ("new_int", True),
        ("new_datetime", True),
    ]
    assert field_infos == expected_field_infos

    # check that it is possible to update and delete such columns
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.find_field(title="old_int").update(title="renamed_int"),
        ],
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert ds.find_field(title="renamed_int") is not None
    assert ds.result_schema["formula_1"].formula == "[renamed_int] + 1"

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.find_field(title="renamed_int").delete(),
            ds.find_field(title="old_datetime").delete(),
        ],
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 4

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.find_field(title="formula_1").delete(),
            ds.find_field(title="formula_2").delete(),
        ],
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 2


def test_aggregation_consistency_with_unknown_dimensions(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    # check aggregation and another field at same level
    # - should assume all non-aggregated fields are in GROUP BY
    ds.result_schema["New Field"] = ds.field(formula='CONCAT([Order Date], " - ", SUM([Sales]))')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors


def test_add_component_with_reserved_name(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    def assert_not_unique_component_id_error(resp: HttpDatasetApiResponse):
        assert resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp
        assert resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL"
        assert resp.json["message"].startswith("Cannot use a reserved ID")

    ds.result_schema["field_1"] = ds.source_avatars[0].field(source="Profit")
    reserved_ids = (e.value for e in TopLevelComponentId)
    for reserved_id in reserved_ids:
        # add field
        ds.result_schema["bad_formula"] = ds.field(formula="[field_1] + 1", id=reserved_id)
        ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
        assert_not_unique_component_id_error(ds_resp)

        # update field
        updates = [ds.result_schema["field_1"].update(new_id=reserved_id)]
        ds_resp = api_v1.apply_updates(dataset=ds, updates=updates, fail_ok=True)
        assert_not_unique_component_id_error(ds_resp)

        # add obligatory filter
        ds_resp = api_v1.apply_updates(
            dataset=ds,
            fail_ok=True,
            updates=[
                ObligatoryFilter(
                    id=reserved_id,
                    field_guid=ds.result_schema[0].id,
                    default_filters=[
                        WhereClause(
                            column=ds.result_schema[0].id, operation=WhereClauseOperation.STARTSWITH, values=["..."]
                        ),
                    ],
                ).add()
            ],
        )
        assert_not_unique_component_id_error(ds_resp)

        # add source avatar
        source_avatar = ds.sources[0].avatar()
        source_avatar.id = reserved_id
        ds.source_avatars["avatar_1"] = source_avatar
        ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
        assert_not_unique_component_id_error(ds_resp)
        ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

        # add avatar relation
        ds.source_avatars["avatar_1"] = ds.sources[0].avatar()
        ds.source_avatars["avatar_2"] = ds.sources[0].avatar()
        ds.avatar_relations["relation_1"] = (
            ds.source_avatars["avatar_1"].join(ds.source_avatars["avatar_2"]).on(ds.col("Profit") == ds.col("Profit"))
        )
        ds_resp = api_v1.apply_updates(dataset=ds, updates=updates, fail_ok=True)
        assert_not_unique_component_id_error(ds_resp)
        ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset


def test_clone_field(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    first_field = [f for f in ds.result_schema if f.data_type == UserDataType.date][0]
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            {
                "action": "clone_field",
                "field": {
                    "guid": str(uuid.uuid4()),
                    "from_guid": first_field.id,
                    "title": str(uuid.uuid4()),
                },
            }
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    new_field = ds.result_schema[0]
    patched_old_field = attr.evolve(first_field, id=new_field.id, title=new_field.title)
    assert patched_old_field == new_field

    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            {
                "action": "clone_field",
                "field": {
                    "guid": str(uuid.uuid4()),
                    "from_guid": first_field.id,
                    "title": str(uuid.uuid4()),
                    "cast": UserDataType.integer.name,
                    "aggregation": AggregationFunction.sum.name,
                },
            }
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    new_field = ds.result_schema[0]
    assert new_field.source == first_field.source and new_field.formula == first_field.formula
    assert new_field.cast == UserDataType.integer
    assert new_field.aggregation == AggregationFunction.sum


def test_field_dependencies(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    ds.result_schema["MainField"] = ds.source_avatars[0].field(source="Profit")
    ds.result_schema["Formula1"] = ds.field(formula="[MainField] + 1")
    ds.result_schema["Formula2"] = ds.field(formula="[Formula1] + 1")
    ds.result_schema["Formula3"] = ds.field(formula="[Formula2] + [MainField]")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    refs_by_field_id = {
        item["dep_field_id"]: set(item["ref_field_ids"]) for item in ds.result_schema_aux.inter_dependencies["deps"]
    }
    assert refs_by_field_id == {
        ds.result_schema["Formula1"].id: {ds.result_schema["MainField"].id},
        ds.result_schema["Formula2"].id: {ds.result_schema["Formula1"].id},
        ds.result_schema["Formula3"].id: {ds.result_schema["Formula2"].id, ds.result_schema["MainField"].id},
    }


def test_component_error_cleanup_after_automatic_field_deletion(api_v1, connection_id, clickhouse_table):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(clickhouse_table))

    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    ds.result_schema["Artificial Field"] = ds.source_avatars["avatar_1"].field(source="nonexistent")
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert not ds.result_schema["Artificial Field"].valid
    assert len(ds.component_errors.items) == 1
    assert ds.component_errors.items[0].id == ds.result_schema["Artificial Field"].id

    # Delete avatar and source -> all fields are deleted automatically,
    # so should the corresponding component errors
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        fail_ok=True,
        updates=[
            ds.source_avatars["avatar_1"].delete(),
            ds.sources["source_1"].delete(),
        ],
    )
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0


def test_validation_on_phantom_errors(api_v1, connection_id, clickhouse_table, default_sync_usm):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(clickhouse_table))

    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds = api_v1.apply_updates(dataset=ds, fail_ok=False).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    dsi = default_sync_usm.get_by_id(ds.id)
    dsi.error_registry.add_error(
        id="some_fake_id",
        type=ComponentType.field,
        message="Some fake id to be excluded",
        code=DatasetError.err_code,
    )
    default_sync_usm.save(dsi)

    ds_with_err = api_v1.load_dataset(ds).dataset
    ds = api_v1.apply_updates(ds_with_err, [], fail_ok=False).dataset
    assert len(ds.component_errors.items) == 0


def test_none_as_connection_id(api_v1, connection_id, clickhouse_table):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=None, **data_source_settings_from_table(clickhouse_table))
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL"


def test_field_from_nonexistent_avatar(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    original_field_cnt = len(ds.result_schema)

    ds.result_schema["My Field"] = ds.field(avatar_id="nonexistent", source="Profit")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"  # non-fatal
    ds = ds_resp.dataset
    assert not ds.result_schema["My Field"].valid

    ds_resp = api_v1.apply_updates(
        dataset=ds, updates=[ds.find_field(title="My Field").update(title="My Field Renamed")], fail_ok=True
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"  # non-fatal
    ds = ds_resp.dataset
    assert not ds.find_field(title="My Field Renamed").valid

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.find_field(title="My Field Renamed").delete()], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    new_field_cnt = len(ds.result_schema)
    assert new_field_cnt == original_field_cnt


def test_formula_null(api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema["Null Field"] = ds.field(avatar_id="nonexistent", formula="NULL")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK
    assert ds_resp.dataset.result_schema["Null Field"].data_type == UserDataType.string
