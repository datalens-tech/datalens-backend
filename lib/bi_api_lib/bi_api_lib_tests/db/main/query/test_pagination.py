from typing import Optional

from bi_constants.enums import FieldRole

from bi_core.us_dataset import Dataset

from bi_query_processing.legend.field_legend import Legend, LegendItem, FieldObjSpec, RowRoleSpec
from bi_query_processing.legend.block_legend import BlockSpec, BlockLegend, BlockLegendMeta
from bi_query_processing.pagination.paginator import QueryPaginator
from bi_query_processing.merging.primitives import MergedQueryDataStream, MergedQueryDataRow, MergedQueryMetaInfo


def _make_legend(dataset: Dataset) -> Legend:
    field = dataset.result_schema[0]
    legend = Legend(items=[
        LegendItem(
            legend_item_id=0,
            obj=FieldObjSpec(id=field.guid, title=field.title),
            role_spec=RowRoleSpec(role=FieldRole.row),
            field_type=field.type, data_type=field.data_type,
        ),
    ])
    return legend


def _make_block_legend(
        dataset: Dataset,
        block_limit: Optional[int] = None, block_offset: Optional[int] = None,
        glob_limit: Optional[int] = None, glob_offset: Optional[int] = None,
) -> BlockLegend:
    legend = _make_legend(dataset)
    block_legend = BlockLegend(
        blocks=[
            BlockSpec(
                block_id=0,
                parent_block_id=0,
                legend=legend,
                limit=block_limit,
                offset=block_offset,
            ),
        ],
        meta=BlockLegendMeta(
            limit=glob_limit,
            offset=glob_offset,
        )
    )
    return block_legend


def test_pre_paginate_single_block(dataset_id, default_sync_usm):
    dataset = default_sync_usm.get_by_id(dataset_id)
    paginator = QueryPaginator()
    pre_paginator = paginator.get_pre_paginator()

    block_legend = _make_block_legend(dataset, glob_limit=None, glob_offset=None, block_limit=None, block_offset=None)

    # No pagination
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit is None
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # block limit
    block_legend = _make_block_legend(dataset, glob_limit=None, glob_offset=None, block_limit=7, block_offset=None)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit == 7
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # global limit (pagination is moved from global to block level)
    block_legend = _make_block_legend(dataset, glob_limit=7, glob_offset=None, block_limit=None, block_offset=None)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit == 7
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # global and block pagination (nothing happens)
    block_legend = _make_block_legend(dataset, glob_limit=7, glob_offset=None, block_limit=None, block_offset=3)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit is None
    assert block_legend.blocks[0].offset == 3
    assert block_legend.meta.limit == 7
    assert block_legend.meta.offset is None


def test_post_paginate_single_block(dataset_id, default_sync_usm):
    dataset = default_sync_usm.get_by_id(dataset_id)
    paginator = QueryPaginator()
    post_paginator = paginator.get_post_paginator()

    legend = _make_legend(dataset)
    legend_item_ids = [item.legend_item_id for item in legend.items]

    rows = [
        MergedQueryDataRow(data=(), legend_item_ids=())
        for i in range(10)
    ]
    assert len(rows) == 10

    stream = MergedQueryDataStream(
        rows=rows, legend=legend, legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=None, offset=None))
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 10

    stream = MergedQueryDataStream(
        rows=rows, legend=legend, legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=7, offset=None))
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 7

    stream = MergedQueryDataStream(
        rows=rows, legend=legend, legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=None, offset=2))
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 8

    stream = MergedQueryDataStream(
        rows=rows, legend=legend, legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=5, offset=2))
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 5
