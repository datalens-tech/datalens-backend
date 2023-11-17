from typing import Optional

from dl_constants.enums import (
    FieldRole,
    FieldType,
    UserDataType,
)
from dl_query_processing.legend.block_legend import (
    BlockLegend,
    BlockLegendMeta,
    BlockSpec,
)
from dl_query_processing.legend.field_legend import (
    FieldObjSpec,
    Legend,
    LegendItem,
    RowRoleSpec,
)
from dl_query_processing.merging.primitives import (
    MergedQueryDataRow,
    MergedQueryDataStream,
    MergedQueryMetaInfo,
)
from dl_query_processing.pagination.paginator import QueryPaginator


def _make_legend() -> Legend:
    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=0,
                obj=FieldObjSpec(id='guid', title='title'),
                role_spec=RowRoleSpec(role=FieldRole.row),
                field_type=FieldType.DIMENSION,
                data_type=UserDataType.unsupported,
            ),
        ]
    )
    return legend


def _make_block_legend(
    block_limit: Optional[int] = None,
    block_offset: Optional[int] = None,
    glob_limit: Optional[int] = None,
    glob_offset: Optional[int] = None,
) -> BlockLegend:
    legend = _make_legend()
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
        ),
    )
    return block_legend


def test_pre_paginate_single_block():
    paginator = QueryPaginator()
    pre_paginator = paginator.get_pre_paginator()

    block_legend = _make_block_legend(glob_limit=None, glob_offset=None, block_limit=None, block_offset=None)

    # No pagination
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit is None
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # block limit
    block_legend = _make_block_legend(glob_limit=None, glob_offset=None, block_limit=7, block_offset=None)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit == 7
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # global limit (pagination is moved from global to block level)
    block_legend = _make_block_legend(glob_limit=7, glob_offset=None, block_limit=None, block_offset=None)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit == 7
    assert block_legend.blocks[0].offset is None
    assert block_legend.meta.limit is None
    assert block_legend.meta.offset is None

    # global and block pagination (nothing happens)
    block_legend = _make_block_legend(glob_limit=7, glob_offset=None, block_limit=None, block_offset=3)
    block_legend = pre_paginator.pre_paginate(block_legend)
    assert block_legend.blocks[0].limit is None
    assert block_legend.blocks[0].offset == 3
    assert block_legend.meta.limit == 7
    assert block_legend.meta.offset is None


def test_post_paginate_single_block():
    paginator = QueryPaginator()
    post_paginator = paginator.get_post_paginator()

    legend = _make_legend()
    legend_item_ids = [item.legend_item_id for item in legend.items]

    rows = [MergedQueryDataRow(data=(), legend_item_ids=())] * 10
    assert len(rows) == 10

    stream = MergedQueryDataStream(
        rows=rows,
        legend=legend,
        legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=None, offset=None),
    )
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 10

    stream = MergedQueryDataStream(
        rows=rows,
        legend=legend,
        legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=7, offset=None),
    )
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 7

    stream = MergedQueryDataStream(
        rows=rows,
        legend=legend,
        legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=None, offset=2),
    )
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 8

    stream = MergedQueryDataStream(
        rows=rows,
        legend=legend,
        legend_item_ids=legend_item_ids,
        meta=MergedQueryMetaInfo(blocks=[], limit=5, offset=2),
    )
    stream = post_paginator.post_paginate(stream)
    assert len(list(stream.rows)) == 5
