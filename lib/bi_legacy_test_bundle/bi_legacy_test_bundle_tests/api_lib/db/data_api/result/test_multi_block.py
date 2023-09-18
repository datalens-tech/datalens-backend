from __future__ import annotations

from http import HTTPStatus
from typing import (
    List,
    Optional,
)

from dl_api_client.dsmaker.primitives import (
    AfterBlockPlacement,
    BlockSpec,
    RootBlockPlacement,
)
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_constants.enums import WhereClauseOperation


def test_identical_blocks(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
        },
    )

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Category").as_req_legend_item(block_id=0, legend_item_id=4),
            ds.find_field(title="Sales Sum").as_req_legend_item(block_id=0, legend_item_id=8),
            ds.find_field(title="Category").as_req_legend_item(block_id=1, legend_item_id=13),
            ds.find_field(title="Sales Sum").as_req_legend_item(block_id=1, legend_item_id=7),
        ],
        order_by=[
            ds.find_field(title="Category").asc,
        ],
        blocks=[
            BlockSpec(block_id=0, placement=RootBlockPlacement()),
            BlockSpec(block_id=1, parent_block_id=0, placement=AfterBlockPlacement()),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    rows = get_data_rows(result_resp)
    data_rows_0 = rows[: len(rows) // 2]
    data_rows_1 = rows[len(rows) // 2 :]

    assert result_resp.json["result_data"][0]["rows"][0]["legend"] == [4, 8]
    assert result_resp.json["result_data"][0]["rows"][-1]["legend"] == [13, 7]

    assert data_rows_0 == data_rows_1


def test_blocks_with_reversed_order(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
        },
    )

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Category").as_req_legend_item(legend_item_id=4),
            ds.find_field(title="Sales Sum").as_req_legend_item(legend_item_id=8),
        ],
        order_by=[
            ds.find_field(title="Category").asc.for_block(block_id=0),
            ds.find_field(title="Category").desc.for_block(block_id=1),
        ],
        blocks=[
            BlockSpec(block_id=0, placement=RootBlockPlacement()),
            BlockSpec(block_id=1, parent_block_id=0, placement=AfterBlockPlacement()),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    rows = get_data_rows(result_resp)
    data_rows_0 = rows[: len(rows) // 2]
    data_rows_1 = rows[len(rows) // 2 :]
    assert result_resp.json["result_data"][0]["rows"][0]["legend"] == [4, 8]
    assert result_resp.json["result_data"][0]["rows"][-1]["legend"] == [4, 8]

    assert data_rows_0 == list(reversed(data_rows_1))


def test_blocks_with_different_filters(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
        },
    )

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Category").as_req_legend_item(legend_item_id=4),
            ds.find_field(title="Sales Sum").as_req_legend_item(legend_item_id=8),
        ],
        filters=[
            ds.find_field(title="Category")
            .filter(op=WhereClauseOperation.EQ, values=["Technology"])
            .for_block(block_id=0),
            ds.find_field(title="Category")
            .filter(op=WhereClauseOperation.NE, values=["Technology"])
            .for_block(block_id=1),
        ],
        blocks=[
            BlockSpec(block_id=0, placement=RootBlockPlacement()),
            BlockSpec(block_id=1, parent_block_id=0, placement=AfterBlockPlacement()),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    rows = get_data_rows(result_resp)
    data_rows_0 = rows[: len(rows) // 2]
    data_rows_1 = rows[len(rows) // 2 :]
    assert result_resp.json["result_data"][0]["rows"][0]["legend"] == [4, 8]
    assert result_resp.json["result_data"][0]["rows"][-1]["legend"] == [4, 8]

    cats_0 = {row[0] for row in data_rows_0}
    cats_1 = {row[0] for row in data_rows_1}
    assert "Technology" in cats_0
    assert "Technology" not in cats_1


def test_multiblock_limit_offset(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
        },
    )

    def get_result(block_cnt: int, limit: Optional[int] = None, offset: Optional[int] = None) -> List[list]:
        result_resp = data_api_v2.get_result(
            dataset=ds,
            fields=[
                *[ds.find_field(title="City").as_req_legend_item(block_id=block_id) for block_id in range(block_cnt)],
                ds.find_field(title="Sales Sum"),
            ],
            order_by=[
                ds.find_field(title="City").asc,
            ],
            blocks=[
                BlockSpec(block_id=0, placement=RootBlockPlacement()),
                *[BlockSpec(block_id=block_id, placement=AfterBlockPlacement()) for block_id in range(1, block_cnt)],
            ],
            limit=limit,
            offset=offset,
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        return get_data_rows(result_resp)

    single_block_data_rows = get_result(block_cnt=1)
    single_block_len = len(single_block_data_rows)

    limit = single_block_len * 2 + 5
    offset = 7
    data_rows = get_result(block_cnt=3, limit=limit, offset=offset)
    assert len(data_rows) == limit
    assert data_rows[0][0] == single_block_data_rows[offset][0]
