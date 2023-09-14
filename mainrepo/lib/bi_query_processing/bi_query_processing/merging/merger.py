from typing import (
    List,
    Optional,
)

from bi_query_processing.legend.block_legend import RootBlockPlacement
from bi_query_processing.merging.binary_merger import make_binary_merger
from bi_query_processing.merging.primitives import (
    MergedQueryBlockMetaInfo,
    MergedQueryDataRow,
    MergedQueryDataRowIterable,
    MergedQueryDataStream,
    MergedQueryMetaInfo,
)
from bi_query_processing.postprocessing.primitives import (
    PostprocessedQueryBlock,
    PostprocessedQueryUnion,
)


class DataStreamMerger:
    """
    Merges several postprocessed data streams into one
    vertically (rows from blocks are intermixed)
    and/or horizontally (row from one stream is combined with a row from another stream).
    """

    def normalize_stream(self, block: PostprocessedQueryBlock) -> MergedQueryDataRowIterable:
        for postprocessed_row in block.postprocessed_query.postprocessed_data:
            yield MergedQueryDataRow(data=postprocessed_row, legend_item_ids=block.legend_item_ids)

    def merge(self, postprocessed_query_union: PostprocessedQueryUnion) -> MergedQueryDataStream:
        legend_item_ids: Optional[List[int]] = None
        if len(postprocessed_query_union.blocks) == 1:
            legend_item_ids = postprocessed_query_union.blocks[0].legend_item_ids

        # Find the root block/stream
        root_block = next(
            iter(block for block in postprocessed_query_union.blocks if isinstance(block.placement, RootBlockPlacement))
        )
        data_stream: MergedQueryDataRowIterable = self.normalize_stream(root_block)

        merged_target_connection_ids: set[str] = set()
        for block in postprocessed_query_union.blocks:
            merged_target_connection_ids |= block.postprocessed_query.meta.target_connection_ids
            if block is not root_block:
                binary_merger = make_binary_merger(placement=block.placement)
                child_data_stream = self.normalize_stream(block)
                data_stream = binary_merger.merge_two_streams(
                    parent_stream=data_stream,
                    child_stream=child_data_stream,
                )

        merged_data_stream = MergedQueryDataStream(
            legend=postprocessed_query_union.legend,
            rows=data_stream,
            legend_item_ids=legend_item_ids,
            meta=MergedQueryMetaInfo(
                blocks=[
                    MergedQueryBlockMetaInfo(
                        block_id=block.block_id,
                        query_type=block.postprocessed_query.meta.query_type,
                        debug_query=block.postprocessed_query.meta.debug_query,
                    )
                    for block in postprocessed_query_union.blocks
                ],
                offset=postprocessed_query_union.offset,
                limit=postprocessed_query_union.limit,
                target_connection_ids=merged_target_connection_ids,
            ),
        )
        return merged_data_stream
