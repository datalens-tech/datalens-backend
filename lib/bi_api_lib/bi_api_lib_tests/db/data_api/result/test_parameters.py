from __future__ import annotations

import pytest
from http import HTTPStatus

from bi_api_client.dsmaker.primitives import IntegerParameterValue, RangeParameterValueConstraint
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset, add_parameters_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


@pytest.mark.parametrize(
    ('multiplier', 'expected_status_code'),
    (
        (None, HTTPStatus.OK),
        (2, HTTPStatus.OK),
        (5, HTTPStatus.OK),
        (-1, HTTPStatus.BAD_REQUEST),
    )
)
def test_parameter_in_formula(api_v1, data_api_v2, dataset_id, multiplier, expected_status_code):
    default_multiplier = 1

    ds = add_parameters_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        parameters={
            'Multiplier': (
                IntegerParameterValue(default_multiplier),
                RangeParameterValueConstraint(min=IntegerParameterValue(default_multiplier))
            ),
        }
    )
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset=ds, formulas={
        'Multiplied Quantity': '[Quantity] * [Multiplier]',
    })

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Quantity').as_req_legend_item(),
            ds.find_field(title='Multiplier').as_req_legend_item(),
            ds.find_field(title='Multiplied Quantity').as_req_legend_item(),
        ],
        parameters=[
            ds.find_field(title='Multiplier').parameter_value(multiplier),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == expected_status_code, result_resp.json

    if expected_status_code == HTTPStatus.OK:
        data_rows = get_data_rows(result_resp)
        for row in data_rows:
            assert int(row[1]) == (multiplier or default_multiplier)
            assert int(row[0]) * int(row[1]) == int(row[2])


def test_parameter_no_constraint(api_v1, data_api_v2, dataset_id):
    ds = add_parameters_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        parameters={
            'Param': (IntegerParameterValue(0), None),
        }
    )
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset=ds, formulas={
        'Value': '[Param]',
    })

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Value').as_req_legend_item(),
        ],
        parameters=[
            ds.find_field(title='Param').parameter_value(1),
        ],
        limit=1,
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    assert int(get_data_rows(result_resp)[0][0]) == 1
