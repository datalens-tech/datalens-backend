from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bi_constants.enums import FieldRole

from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows
from bi_api_client.dsmaker.api.data_api import HttpDataApiResponse


def test_pagination_single_block_optimization(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    def send_request(offset: Optional[int], limit: Optional[int]) -> HttpDataApiResponse:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='City'),
                ds.find_field(title='Sales Sum'),
            ],
            order_by=[
                ds.find_field(title='Sales Sum'),
            ],
            row_count_hard_limit=20,
            offset=offset, limit=limit,
            fail_ok=True,
        )
        return result_resp

    result_resp = send_request(offset=None, limit=None)
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.ROW_COUNT_LIMIT'

    result_resp = send_request(offset=15, limit=10)
    assert result_resp.status_code == HTTPStatus.OK
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 10


def test_pagination_multiblock_optimization(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    def send_request(offset: Optional[int], limit: Optional[int]) -> HttpDataApiResponse:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                # Main Data
                ds.find_field(title='City').as_req_legend_item(role=FieldRole.row, block_id=0),
                ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.row, block_id=0),
                # Subtotals
                ds.placeholder_as_req_legend_item(role=FieldRole.template, template='-', block_id=1),
                ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.total, block_id=1),
            ],
            order_by=[
                ds.find_field(title='Sales Sum'),
            ],
            row_count_hard_limit=20,
            offset=offset, limit=limit,
            fail_ok=True,
        )
        return result_resp

    result_resp = send_request(offset=None, limit=None)
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.ROW_COUNT_LIMIT'

    result_resp = send_request(offset=5, limit=10)
    assert result_resp.status_code == HTTPStatus.OK
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 10
