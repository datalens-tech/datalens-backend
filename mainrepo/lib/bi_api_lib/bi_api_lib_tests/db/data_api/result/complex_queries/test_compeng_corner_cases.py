from __future__ import annotations

from http import HTTPStatus

import pytest

from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


def test_zero_division_in_compeng(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2

    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Measure Reg Div': '1 / (SUM(SUM([Sales]) AMONG) * 0)',
        'Measure Int Div': 'DIV(1, SUM(COUNT([Sales]) AMONG) * 0)',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Measure Reg Div'),
            ds.find_field(title='Measure Int Div'),
        ],
        fail_ok=True,
    )

    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 1
    row = data_rows[0]
    assert row[0] is None
    assert row[1] is None


def test_integer_division_in_compeng(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2

    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Measure': 'COUNT([Sales])',
        'Fraction': 'DB_CAST([Measure], "Int64") / DB_CAST(SUM([Measure] TOTAL), "integer")',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='City'),
            ds.find_field(title='Fraction'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) > 1

    fraction_sum = sum(float(row[1]) for row in data_rows)
    assert pytest.approx(fraction_sum) == 1.0
