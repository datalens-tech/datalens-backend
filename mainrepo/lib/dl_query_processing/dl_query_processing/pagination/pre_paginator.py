from typing import (
    Optional,
    TypeVar,
)

from dl_query_processing.legend.block_legend import BlockLegend

_VAL_TV = TypeVar("_VAL_TV")


def ifnull(value: _VAL_TV, null_value: _VAL_TV) -> _VAL_TV:
    if value is not None:
        return value
    return null_value


class QueryPrePaginator:
    """
    Prepares and optimizes query union and individual queries
    for pagination in SQL and postprocessing by tweaking the objects' attributes.
    """

    def _paginate_block(
        self,
        block_legend: BlockLegend,
        block_ind: int,
        limit: Optional[int],
        offset: Optional[int],
    ) -> BlockLegend:
        """
        Set pagination for block specified by index ``block_ind``
        and replace it in the block legend with the paginated copy.
        """
        updated_block = block_legend.blocks[block_ind].clone(
            limit=limit,
            offset=offset,
        )
        updated_blocks = block_legend.blocks[:block_ind] + [updated_block] + block_legend.blocks[block_ind + 1 :]
        return block_legend.clone(blocks=updated_blocks)

    def pre_paginate(self, block_legend: BlockLegend) -> BlockLegend:
        # Handle single-block case.
        # Just paginate the block in the source to avoid selecting too much data
        if len(block_legend.blocks) == 1:
            only_block = block_legend.blocks[0]
            if (
                # Block's own pagination is not specified
                (only_block.limit is None and only_block.offset is None)
                # But global pagination is
                and (block_legend.meta.limit is not None or block_legend.meta.offset is not None)
            ):
                # So apply global pagination to the block
                block_legend = self._paginate_block(
                    block_legend=block_legend,
                    block_ind=0,
                    limit=block_legend.meta.limit,
                    offset=block_legend.meta.offset,
                )
                # and remove global pagination from the legend
                block_legend = block_legend.clone(
                    meta=block_legend.meta.clone(limit=None, offset=None)  # disable global pagination
                )

        # Multi-block case.
        # We can't use the offset on the first block because it might be empty
        # and then the offset should be applied to the second block and so on,
        # and we don't know how much of it should have been consumed by each block.
        # But what we can do is apply the overall offset+limit as a limit to every single block.
        if len(block_legend.blocks) > 1 and block_legend.meta.limit is not None:
            master_limit = block_legend.meta.limit
            if block_legend.meta.offset is not None:
                master_limit += block_legend.meta.offset

            for block_ind in range(len(block_legend.blocks)):
                block_legend = self._paginate_block(
                    block_legend=block_legend,
                    block_ind=block_ind,
                    limit=master_limit,
                    offset=None,
                )

        return block_legend
