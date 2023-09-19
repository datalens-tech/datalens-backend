from __future__ import annotations

import abc
from typing import (
    List,
    Optional,
    Tuple,
    TypeVar,
)

import attr

from dl_api_commons.reporting.registry import ReportingRegistry
from dl_api_lib.query.formalization.field_resolver import FieldResolver
from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_specs import (
    RawAfterBlockPlacement,
    RawBlockSpec,
    RawQuerySpecUnion,
    RawRootBlockPlacement,
)
from dl_constants.enums import (
    FieldRole,
    FieldType,
    NotificationType,
)
from dl_core.reporting.notifications import get_notification_record
from dl_core.us_dataset import Dataset
from dl_query_processing.enums import (
    EmptyQueryMode,
    QueryType,
)
import dl_query_processing.exc
from dl_query_processing.legend.block_legend import (
    AfterBlockPlacement,
    BlockLegend,
    BlockLegendMeta,
    BlockPlacement,
    BlockSpec,
    DimensionSpec,
    DimensionValueSpec,
    DispersedAfterBlockPlacement,
    RootBlockPlacement,
)
from dl_query_processing.legend.field_legend import (
    Legend,
    TreeRoleSpec,
)


_VAL_TV = TypeVar("_VAL_TV")


def ifnull(value: _VAL_TV, null_value: _VAL_TV) -> _VAL_TV:
    if value is not None:
        return value
    return null_value


@attr.s
class BlockFormalizer(abc.ABC):
    _dataset: Dataset = attr.ib(kw_only=True)
    _reporting_registry: Optional[ReportingRegistry] = attr.ib(kw_only=True, default=None)
    _field_resolver: FieldResolver = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._field_resolver = FieldResolver(dataset=self._dataset)

    def _gen_legend_and_ids_for_block(self, legend: Legend, block_id: int) -> Tuple[Legend, List[int]]:
        legend_for_block = legend.limit_to_block(block_id=block_id)
        legend_item_ids = [item.legend_item_id for item in legend_for_block.list_streamable_items()]
        return legend_for_block, legend_item_ids

    def _resolve_block_placement_from_raw_spec(self, raw_block_spec: RawBlockSpec) -> BlockPlacement:
        # Make placement
        placement: BlockPlacement
        if isinstance(raw_block_spec.placement, RawRootBlockPlacement):
            placement = RootBlockPlacement()
        elif isinstance(raw_block_spec.placement, RawAfterBlockPlacement):
            dimension_values: Optional[List[DimensionValueSpec]] = None
            if raw_block_spec.placement.dimension_values is not None:
                dimension_values = [
                    DimensionValueSpec(
                        legend_item_id=dim_val_spec.legend_item_id,
                        value=dim_val_spec.value,
                    )
                    for dim_val_spec in raw_block_spec.placement.dimension_values
                ]
            placement = AfterBlockPlacement(dimension_values=dimension_values)
        else:
            raise TypeError(f"Unsupported placement type: {type(raw_block_spec.placement).__name__}")

        return placement

    def _generate_default_block_spec(
        self,
        block_id: int,
        legend: Legend,
        raw_query_spec_union: RawQuerySpecUnion,
        raw_block_spec: Optional[RawBlockSpec] = None,
        main_block: Optional[BlockSpec] = None,
    ) -> BlockSpec:
        legend_for_block, legend_item_ids = self._gen_legend_and_ids_for_block(legend=legend, block_id=block_id)

        streamable_items = legend_for_block.list_streamable_items()

        empty_query_mode = EmptyQueryMode.error
        if streamable_items and all(item.role_spec.role == FieldRole.template for item in streamable_items):
            # Always return an empty row for template-only query
            empty_query_mode = EmptyQueryMode.empty_row

        query_type = raw_query_spec_union.meta.query_type
        if any(item.role_spec.role in (FieldRole.total, FieldRole.template) for item in streamable_items):
            query_type = QueryType.totals

        parent_block_id: Optional[int]
        limit: Optional[int]
        offset: Optional[int]
        row_count_hard_limit: Optional[int]
        placement: BlockPlacement

        if raw_block_spec is not None:
            parent_block_id = raw_block_spec.parent_block_id
            limit = raw_block_spec.limit
            offset = raw_block_spec.offset
            row_count_hard_limit = ifnull(
                raw_block_spec.row_count_hard_limit,
                raw_query_spec_union.meta.row_count_hard_limit,
            )
            # Make placement
            placement = self._resolve_block_placement_from_raw_spec(raw_block_spec)
        else:
            parent_block_id = None
            limit = None
            offset = None
            row_count_hard_limit = raw_query_spec_union.meta.row_count_hard_limit
            placement = self._resolve_block_placement_from_legend(
                legend_for_block,
                main_block=main_block,
                query_type=raw_query_spec_union.meta.query_type,
            )

        return BlockSpec(
            block_id=block_id,
            parent_block_id=parent_block_id,
            placement=placement,
            legend_item_ids=legend_item_ids,
            legend=legend_for_block,
            limit=limit,
            offset=offset,
            query_type=query_type,
            row_count_hard_limit=row_count_hard_limit,
            group_by_policy=raw_query_spec_union.group_by_policy,
            ignore_nonexistent_filters=raw_query_spec_union.ignore_nonexistent_filters,
            disable_rls=raw_query_spec_union.disable_rls,
            allow_measure_fields=raw_query_spec_union.allow_measure_fields,
            empty_query_mode=empty_query_mode,
        )

    def _resolve_block_placement_from_legend(
        self,
        legend_for_block: Legend,
        main_block: Optional[BlockSpec],
        query_type: QueryType,
    ) -> BlockPlacement:
        streamable_items = legend_for_block.list_streamable_items()
        if streamable_items and all(item.role_spec.role == FieldRole.template for item in streamable_items):
            # All items are templates
            # (Most likely a query for totals, but without any measures)
            # The only thing we can do is place it at the end
            return AfterBlockPlacement()

        total_items = legend_for_block.list_for_role(FieldRole.total)
        tree_items = legend_for_block.list_for_role(FieldRole.tree)

        if tree_items and total_items:
            raise dl_query_processing.exc.BlockItemCompatibilityError(
                "Got tree and totals in one block. They are incompatible"
            )

        if total_items and query_type == QueryType.result:
            assert main_block is not None
            # This is a block of totals
            main_legend_item_ids = main_block.legend_item_ids
            # There must be the same number of items as in  the main block
            if len(streamable_items) != len(main_legend_item_ids):
                raise dl_query_processing.exc.UnevenBlockColumnCountError(
                    "Blocks have different column counts. "
                    f"Main block: {len(main_legend_item_ids)}, secondary: {len(streamable_items)}"
                )
            parent_dim_specs: list[DimensionSpec] = []
            child_dim_specs: list[DimensionSpec] = []
            for item_idx, item in enumerate(streamable_items):
                if item.field_type == FieldType.DIMENSION and item.role_spec.role != FieldRole.template:
                    parent_dim_specs.append(DimensionSpec(legend_item_id=main_legend_item_ids[item_idx]))
                    child_dim_specs.append(DimensionSpec(legend_item_id=item.legend_item_id))

            return DispersedAfterBlockPlacement(
                parent_dimensions=parent_dim_specs,
                child_dimensions=child_dim_specs,
            )

        if total_items:  # pivot, maybe others
            # No point in doing complex data merging if it will all be re-mixed in pivot table.
            # So just stick totals at the end.
            return AfterBlockPlacement()

        if tree_items:
            if len(tree_items) > 1:
                raise dl_query_processing.exc.MultipleTreeError(
                    "Multiple trees detected in block. This is not supported"
                )
            # This is a tree block
            tree_item = tree_items[0]
            tree_spec = tree_item.role_spec
            assert isinstance(tree_spec, TreeRoleSpec)
            return AfterBlockPlacement(dimension_values=tree_spec.dimension_values)

        return RootBlockPlacement()

    def validate_block_legend(self, block_legend: BlockLegend) -> None:
        root_cnt = 0
        for block_spec in block_legend.blocks:
            is_root = isinstance(block_spec.placement, RootBlockPlacement)
            if is_root:
                root_cnt += 1

        if root_cnt > 1:
            raise dl_query_processing.exc.MultipleRootBlockError()
        if root_cnt < 1:
            raise dl_query_processing.exc.NoRootBlockError()

    def finalize_block_legend(self, block_legend: BlockLegend) -> BlockLegend:
        if len(block_legend.blocks) > 1:
            first_block, *other_blocks = block_legend.blocks
            if (  # FIXME: separate `QueryType` into `DataRequestType` and `BlockType`
                # Regular main block
                first_block.query_type in {QueryType.result, QueryType.pivot}
                # and all others are (sub)totals
                and all(block.query_type == QueryType.totals for block in other_blocks)
            ):
                filter_items = first_block.legend.list_for_role(FieldRole.filter)
                if any(item.field_type == FieldType.MEASURE for item in filter_items):
                    # And if there are MEASURE filters
                    # (which, for now, we can't support in totals correctly),
                    # then remove totals - that is all blocks except for `first_block`
                    non_total_blocks = [block for block in block_legend.blocks if block.query_type != QueryType.totals]
                    block_legend = block_legend.clone(blocks=non_total_blocks)
                    if self._reporting_registry is not None:
                        self._reporting_registry.save_reporting_record(
                            get_notification_record(NotificationType.totals_removed_due_to_measure_filter)
                        )

        return block_legend

    def make_block_legend(self, raw_query_spec_union: RawQuerySpecUnion, legend: Legend) -> BlockLegend:
        used_ids = set(raw_query_spec_union.get_unique_block_ids())
        id_gen = IdGenerator(used_ids=used_ids)
        blocks: List[BlockSpec] = []

        if not used_ids:
            # There is always at least one block.
            # So if none were explicitly specified, then generate one ID automatically
            id_gen.generate_id()  # It adds to used_ids

        # Create explicitly listed block specs
        for raw_block_spec in raw_query_spec_union.block_specs:
            block_spec = self._generate_default_block_spec(
                block_id=raw_block_spec.block_id,
                raw_block_spec=raw_block_spec,
                legend=legend,
                raw_query_spec_union=raw_query_spec_union,
                main_block=blocks[0] if blocks else None,
            )
            blocks.append(block_spec)

        # Generate block specs not listed explicitly, but referred to in the query legend
        orphaned_ids = used_ids - {block_spec.block_id for block_spec in blocks}
        for block_id in sorted(orphaned_ids):
            block_spec = self._generate_default_block_spec(
                block_id=block_id,
                legend=legend,
                raw_query_spec_union=raw_query_spec_union,
                main_block=blocks[0] if blocks else None,
            )
            blocks.append(block_spec)

        # The above logic ensures that there is at least one block,
        # but to make sure `blocks` is not empty let's
        assert blocks
        # Force the first block to be root
        blocks[0] = blocks[0].clone(placement=RootBlockPlacement())

        block_legend = BlockLegend(
            blocks=blocks,
            meta=BlockLegendMeta(
                limit=raw_query_spec_union.limit,
                offset=raw_query_spec_union.offset,
                row_count_hard_limit=raw_query_spec_union.meta.row_count_hard_limit,
            ),
        )
        block_legend = self.finalize_block_legend(block_legend=block_legend)
        self.validate_block_legend(block_legend=block_legend)
        return block_legend
