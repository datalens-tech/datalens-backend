from __future__ import annotations

from http import HTTPStatus

from bi_constants.enums import WhereClauseOperation

from bi_api_client.dsmaker.primitives import StringParameterValue
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset, add_parameters_to_dataset


def test_ignore_constant_filter(api_v1, data_api_v2, dataset_id):
    const_value = 'qweqwerty'

    ds = add_formulas_to_dataset(
        api_v1=api_v1, dataset_id=dataset_id,
        formulas={
            'Const': f'"{const_value}"',
        }
    )

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='City').as_req_legend_item(),
        ],
        filters=[
            ds.find_field(title='Const').filter(WhereClauseOperation.EQ, [const_value]),
            ds.find_field(title='Category').filter(WhereClauseOperation.EQ, ['Furniture']),
        ],
        limit=1,
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    query = result_resp.json['blocks'][0]['query']
    assert 'Furniture' in query
    assert const_value not in query
    assert 'and' not in query.lower()


def test_ignore_parameter_filter(api_v1, data_api_v2, dataset_id):
    param_value = 'qweqwerty'

    ds = add_parameters_to_dataset(
        api_v1=api_v1, dataset_id=dataset_id,
        parameters={
            'Param': (StringParameterValue(param_value), None),
        }
    )

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='City').as_req_legend_item(),
        ],
        filters=[
            ds.find_field(title='Param').filter(WhereClauseOperation.EQ, [param_value]),
            ds.find_field(title='Category').filter(WhereClauseOperation.EQ, ['Furniture']),
        ],
        limit=1,
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    query = result_resp.json['blocks'][0]['query']
    assert 'Furniture' in query
    assert param_value not in query
    assert 'and' not in query.lower()
