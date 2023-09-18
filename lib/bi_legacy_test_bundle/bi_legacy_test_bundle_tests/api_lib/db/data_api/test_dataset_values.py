from __future__ import annotations

from http import HTTPStatus

from dl_api_client.dsmaker.primitives import (
    Dataset,
    WhereClause,
)
from dl_api_client.dsmaker.shortcuts.range_data import get_range_values
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_constants.enums import WhereClauseOperation


def test_get_dataset_version_values_distinct(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    distinct_resp = data_api.get_distinct(dataset=ds, field=ds.find_field(title="Category"))
    assert distinct_resp.status_code == HTTPStatus.OK, distinct_resp.json
    values = [item[0] for item in get_data_rows(distinct_resp)]
    assert len(values) == 3
    assert values == sorted(set(values))


def test_get_dataset_version_values_distinct_with_nonexistent_filter(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    distinct_resp = data_api.get_distinct(dataset=ds, field=ds.find_field(title="Category"))
    assert distinct_resp.status_code == HTTPStatus.OK, distinct_resp.json
    values = [item[0] for item in get_data_rows(distinct_resp)]

    distinct_resp = data_api.get_distinct(
        dataset=ds,
        field=ds.find_field(title="Category"),
        filters=[WhereClause(column="idontexist", operation=WhereClauseOperation.EQ, values=[0])],
        ignore_nonexistent_filters=True,
    )
    assert distinct_resp.status_code == HTTPStatus.OK, distinct_resp.json
    values_with_filter = [item[0] for item in get_data_rows(distinct_resp)]
    assert len(values_with_filter) == 3
    assert values_with_filter == sorted(set(values_with_filter))
    assert values_with_filter == values


def test_get_dataset_version_values_range(api_v1, data_api_v2, static_dataset_id):
    data_api = data_api_v2
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    range_resp = data_api.get_value_range(dataset=ds, field=ds.find_field(title="Order Date"))
    values = get_range_values(range_resp)
    assert values == ("2014-01-03", "2017-12-30")


def test_range_measure_filter_error(api_v1, data_api_v2, static_dataset_id):
    data_api = data_api_v2
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema["Measure"] = ds.field(formula="SUM([Sales])")

    range_resp = data_api.get_value_range(
        dataset=ds,
        field=ds.find_field(title="Order Date"),
        filters=[
            ds.find_field(title="Measure").filter(WhereClauseOperation.GT, [100]),
        ],
        fail_ok=True,
    )
    assert range_resp.status_code == HTTPStatus.BAD_REQUEST
    assert range_resp.bi_status_code == "ERR.DS_API.FILTER.MEASURE_UNSUPPORTED"


def test_distinct_measure_filter_error(api_v1, data_api_v2, static_dataset_id):
    data_api = data_api_v2
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema["Measure"] = ds.field(formula="SUM([Sales])")

    range_resp = data_api.get_distinct(
        dataset=ds,
        field=ds.find_field(title="Order Date"),
        filters=[
            ds.find_field(title="Measure").filter(WhereClauseOperation.GT, [100]),
        ],
        fail_ok=True,
    )
    assert range_resp.status_code == HTTPStatus.BAD_REQUEST
    assert range_resp.bi_status_code == "ERR.DS_API.FILTER.MEASURE_UNSUPPORTED"
