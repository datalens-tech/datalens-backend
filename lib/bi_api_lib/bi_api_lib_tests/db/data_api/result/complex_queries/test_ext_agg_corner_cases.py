from __future__ import annotations

from http import HTTPStatus

from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


def test_lod_with_const_dim(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Const Dim': '"something"',
        'Measure': 'SUM([Sales] EXCLUDE [Category])',
    })

    result_resp = data_api.get_result(
        dataset=ds, fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Const Dim'),
            ds.find_field(title='Measure'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    assert len(data_rows) == 3
    assert len(set(row[2] for row in data_rows)) == 1  # They should all be the same


def test_lod_with_date_dimension(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Date Dim': 'DB_CAST([Order Date], "Date") + 1',
        'Measure': 'SUM([Sales] EXCLUDE [Category])',
    })

    result_resp = data_api.get_result(
        dataset=ds, fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Date Dim'),
            ds.find_field(title='Measure'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    assert len(data_rows) > 1


def test_duplicate_main_dimensions(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Category 2': '[Category]',
        'Measure': 'AVG(SUM([Sales] INCLUDE [City]))',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Category 2'),
            ds.find_field(title='Measure'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 3  # There are 3 categories
    for row in data_rows:
        assert row[0] == row[1]


def test_duplicate_lod_dimensions(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'City 2': '[City]',
        'Measure Double Dim': 'AVG(SUM([Sales] INCLUDE [City], [City 2]))',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Measure Double Dim'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 3  # There are 3 categories


def test_lod_include_measure(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1, dataset_id=dataset_id,
        formulas={
            'Measure': 'SUM(SUM([Sales] INCLUDE SUM([Profit])))',
        },
        exp_status=HTTPStatus.BAD_REQUEST,
    )

    result_resp = data_api.get_result(
        dataset=ds, fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Measure'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
