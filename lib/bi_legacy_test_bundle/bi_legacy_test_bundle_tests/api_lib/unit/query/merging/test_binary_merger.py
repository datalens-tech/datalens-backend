from dl_query_processing.legend.block_legend import (
    AfterBlockPlacement, DimensionValueSpec,
    DispersedAfterBlockPlacement, DimensionSpec,
)
from dl_query_processing.merging.primitives import MergedQueryDataRow
from dl_query_processing.merging.binary_merger import (
    AfterBinaryStreamMerger, DispersedAfterBinaryStreamMerger,
)


def test_after_binary_stream_merger_no_dims():
    parent_stream = [
        MergedQueryDataRow(data=(12, 34), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
    ]
    child_stream = [
        MergedQueryDataRow(data=('as', 'df'), legend_item_ids=(5, 6)),
        MergedQueryDataRow(data=('zx', 'cv'), legend_item_ids=(5, 6)),
    ]
    binary_merger = AfterBinaryStreamMerger(placement=AfterBlockPlacement())
    output_stream = list(binary_merger.merge_two_streams(parent_stream=parent_stream, child_stream=child_stream))
    assert output_stream == [
        MergedQueryDataRow(data=(12, 34), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=('as', 'df'), legend_item_ids=(5, 6)),
        MergedQueryDataRow(data=('zx', 'cv'), legend_item_ids=(5, 6)),
    ]


def test_after_binary_stream_merger_with_dims():
    parent_stream = [
        MergedQueryDataRow(data=(12, 34), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(6, 7)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(45, 76), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(390, 2), legend_item_ids=(1, 2)),
    ]
    child_stream = [
        MergedQueryDataRow(data=('as', 'df'), legend_item_ids=(5, 6)),
        MergedQueryDataRow(data=('zx', 'cv'), legend_item_ids=(5, 6)),
    ]
    binary_merger = AfterBinaryStreamMerger(
        placement=AfterBlockPlacement(
            dimension_values=[
                DimensionValueSpec(legend_item_id=1, value=56),
                DimensionValueSpec(legend_item_id=2, value=78),
            ],
        ),
    )
    output_stream = list(binary_merger.merge_two_streams(parent_stream=parent_stream, child_stream=child_stream))
    assert output_stream == [
        MergedQueryDataRow(data=(12, 34), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(6, 7)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(56, 78), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=('as', 'df'), legend_item_ids=(5, 6)),
        MergedQueryDataRow(data=('zx', 'cv'), legend_item_ids=(5, 6)),
        MergedQueryDataRow(data=(45, 76), legend_item_ids=(1, 2)),
        MergedQueryDataRow(data=(390, 2), legend_item_ids=(1, 2)),
    ]


def test_dispersed_after_binary_stream_merger():
    parent_stream = [
        MergedQueryDataRow(data=(10, 'abc', 1, 100), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 2, 213), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 3, 489), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 3, 876), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 3, 876), legend_item_ids=(1, 2, 3, 4)),
    ]
    child_stream = [
        MergedQueryDataRow(data=(10, 'abc', '-', 1000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(10, 'def', '-', 2000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(23, 'abc', '-', 3000), legend_item_ids=(5, 6, 7, 8)),
    ]
    binary_merger = DispersedAfterBinaryStreamMerger(
        placement=DispersedAfterBlockPlacement(
            parent_dimensions=[
                DimensionSpec(legend_item_id=1),
                DimensionSpec(legend_item_id=2),
            ],
            child_dimensions=[
                DimensionSpec(legend_item_id=5),
                DimensionSpec(legend_item_id=6),
            ],
        ),
    )
    output_stream = list(binary_merger.merge_two_streams(parent_stream=parent_stream, child_stream=child_stream))
    assert output_stream == [
        MergedQueryDataRow(data=(10, 'abc', 1, 100), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 2, 213), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 3, 489), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', '-', 1000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(10, 'def', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 3, 876), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', '-', 2000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(23, 'abc', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 3, 876), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', '-', 3000), legend_item_ids=(5, 6, 7, 8)),
    ]

    parent_stream = output_stream
    child_stream = [
        MergedQueryDataRow(data=(10, '-', '-', 40000), legend_item_ids=(9, 10, 11, 12)),
        MergedQueryDataRow(data=(23, '-', '-', 70000), legend_item_ids=(9, 10, 11, 12)),
    ]
    binary_merger = DispersedAfterBinaryStreamMerger(
        placement=DispersedAfterBlockPlacement(
            parent_dimensions=[
                DimensionSpec(legend_item_id=1),
            ],
            child_dimensions=[
                DimensionSpec(legend_item_id=9),
            ],
        ),
    )
    output_stream = list(binary_merger.merge_two_streams(parent_stream=parent_stream, child_stream=child_stream))
    assert output_stream == [
        MergedQueryDataRow(data=(10, 'abc', 1, 100), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 2, 213), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', 3, 489), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'abc', '-', 1000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(10, 'def', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', 3, 876), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(10, 'def', '-', 2000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(10, '-', '-', 40000), legend_item_ids=(9, 10, 11, 12)),
        MergedQueryDataRow(data=(23, 'abc', 1, 178), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 2, 231), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', 3, 876), legend_item_ids=(1, 2, 3, 4)),
        MergedQueryDataRow(data=(23, 'abc', '-', 3000), legend_item_ids=(5, 6, 7, 8)),
        MergedQueryDataRow(data=(23, '-', '-', 70000), legend_item_ids=(9, 10, 11, 12)),
    ]
