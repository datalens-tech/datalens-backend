import abc
from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
)

import attr

from dl_constants.enums import QueryBlockPlacementType
from dl_query_processing.legend.block_legend import (
    AfterBlockPlacement,
    BlockPlacement,
    DimensionValueSpec,
    DispersedAfterBlockPlacement,
)
from dl_query_processing.merging.primitives import (
    MergedQueryDataRow,
    MergedQueryDataRowIterable,
)
from dl_query_processing.postprocessing.primitives import PostprocessedValue


@attr.s(frozen=True)
class BinaryStreamMerger(abc.ABC):
    """
    Base class for merger of two data streams: parent and child.
    Used to convert data streams from multiple DB queries into a single output stream.
    """

    placement: BlockPlacement = attr.ib(kw_only=True)

    @abc.abstractmethod
    def merge_two_streams(
        self,
        parent_stream: MergedQueryDataRowIterable,
        child_stream: MergedQueryDataRowIterable,
    ) -> MergedQueryDataRowIterable:
        raise NotImplementedError


@attr.s
class RowMatcher:
    """
    Helper object for optimizing the matching of data rows to specified dimension values
    """

    _dimension_values: List[DimensionValueSpec] = attr.ib(kw_only=True)
    # internal stuff
    _required_legend_item_ids: Set[int] = attr.ib(init=False)
    _match_dim_values: List[PostprocessedValue] = attr.ib(init=False)
    _prev_legend_item_ids: Optional[Iterable[int]] = attr.ib(init=False, default=None)
    _dimension_mask: List[int] = attr.ib(init=False, factory=list)
    _row_has_req_items: bool = attr.ib(init=False, default=False)

    @_required_legend_item_ids.default
    def _make_required_legend_item_ids(self) -> Set[int]:
        return {dim_spec.legend_item_id for dim_spec in self._dimension_values}

    @_match_dim_values.default
    def _make_match_dim_values(self) -> List[PostprocessedValue]:
        return [dim_spec.value for dim_spec in self._dimension_values]

    def match_row(self, row: MergedQueryDataRow) -> bool:
        if row.legend_item_ids is not self._prev_legend_item_ids:
            # Row configuration has changed from the previous row.
            # This is an optimization to avoid generation of dimension_mask for every row.
            if set(row.legend_item_ids).issuperset(self._required_legend_item_ids):
                self._row_has_req_items = True
                self._dimension_mask = [
                    row.legend_item_ids.index(dim_spec.legend_item_id) for dim_spec in self._dimension_values
                ]
            else:
                # Row (and subsequent rows) doesn't contain the required legend items,
                # so don't try to match it.
                self._row_has_req_items = False
                self._dimension_mask = []

            self._prev_legend_item_ids = row.legend_item_ids

        if self._row_has_req_items:  # possible match candidate
            dim_values = [row.data[idx] for idx in self._dimension_mask]
            return dim_values == self._match_dim_values

        return False


class AfterBinaryStreamMerger(BinaryStreamMerger):
    """
    Handle the "after" placement type:
    1. If `dimension_values` is None: simply concatenate the two streams.
    2. If `dimension_values` is not None: insert child stream after
       matching values are found in parent stream.
    """

    @property
    def after_placement(self) -> AfterBlockPlacement:
        assert isinstance(self.placement, AfterBlockPlacement)
        return self.placement

    def _merge_two_streams_no_dims(
        self,
        parent_stream: MergedQueryDataRowIterable,
        child_stream: MergedQueryDataRowIterable,
    ) -> MergedQueryDataRowIterable:
        """
        Just concatenate them.
        """
        yield from parent_stream
        yield from child_stream

    def _merge_two_streams_with_dims(
        self,
        parent_stream: MergedQueryDataRowIterable,
        child_stream: MergedQueryDataRowIterable,
    ) -> MergedQueryDataRowIterable:
        """
        Inspect parent stream values and insert after match.
        """

        dimension_values = self.after_placement.dimension_values
        assert dimension_values is not None
        row_matcher = RowMatcher(dimension_values=dimension_values)
        inserted = False
        previous_row_matched = False

        for row in parent_stream:
            if inserted:
                # Nothing to inspect anymore; yield the rest of the stream
                yield row
                continue

            current_row_matched = row_matcher.match_row(row)
            if previous_row_matched and not current_row_matched:
                yield from child_stream
                inserted = True

            yield row
            previous_row_matched = current_row_matched

        if previous_row_matched and not inserted:
            # We got to the end of the parent stream,
            # but didn't yield the child stream yet
            yield from child_stream

    def merge_two_streams(
        self,
        parent_stream: MergedQueryDataRowIterable,
        child_stream: MergedQueryDataRowIterable,
    ) -> MergedQueryDataRowIterable:
        if self.after_placement.dimension_values is None:
            yield from self._merge_two_streams_no_dims(parent_stream, child_stream)
        else:
            yield from self._merge_two_streams_with_dims(parent_stream, child_stream)


class DispersedAfterBinaryStreamMerger(BinaryStreamMerger):
    """
    In a way similar to the "after" placement type,
    but inserts data rows from `child_stream` separately, one by one
    after the matching rows from parent_stream.
    """

    @property
    def disp_after_placement(self) -> DispersedAfterBlockPlacement:
        assert isinstance(self.placement, DispersedAfterBlockPlacement)
        return self.placement

    def merge_two_streams(
        self,
        parent_stream: MergedQueryDataRowIterable,
        child_stream: MergedQueryDataRowIterable,
    ) -> MergedQueryDataRowIterable:
        dim_indices: Optional[tuple[int, ...]] = None  # This value will be redefined
        dim_values: tuple[Any, ...]
        # Create mapping of child stream values:
        # { <dimension_value_vector>: child_stream_row }
        child_row_map: dict[tuple[Any, ...], MergedQueryDataRow] = {}
        prev_legend_item_ids: Optional[Sequence[int]] = None
        for row in child_stream:
            if row.legend_item_ids is not prev_legend_item_ids:
                # Recreate indices because the legend has changed
                dim_indices = tuple(
                    row.legend_item_ids.index(dim_spec.legend_item_id)
                    for dim_spec in self.disp_after_placement.child_dimensions
                )
                prev_legend_item_ids = row.legend_item_ids

            assert dim_indices is not None
            dim_values = tuple(row.data[idx] for idx in dim_indices)
            child_row_map[dim_values] = row

        # Iterate over parent stream and append child value after each match
        prev_dim_values: Optional[tuple[Any, ...]] = None
        fast_forward_row = False
        expected_parent_dim_set = {dim_spec.legend_item_id for dim_spec in self.disp_after_placement.parent_dimensions}
        for row in parent_stream:
            if row.legend_item_ids is not prev_legend_item_ids:
                prev_legend_item_ids = row.legend_item_ids
                # Check if this row even matches the monitoring criteria
                if not set(row.legend_item_ids).issuperset(expected_parent_dim_set):
                    # fast-forward all rows with this legend
                    fast_forward_row = True

                else:
                    # Recreate indices and values because the legend has changed
                    fast_forward_row = False
                    dim_indices = tuple(
                        row.legend_item_ids.index(dim_spec.legend_item_id)
                        for dim_spec in self.disp_after_placement.parent_dimensions
                    )

            if fast_forward_row:
                yield row
                continue

            assert dim_indices is not None
            dim_values = tuple(row.data[idx] for idx in dim_indices)
            if dim_values != prev_dim_values and prev_dim_values is not None:
                # Values of monitored dimensions have changed.
                # This is where we add the corresponding value from the child stream
                child_stream_row = child_row_map.pop(prev_dim_values, None)
                if child_stream_row is not None:
                    yield child_stream_row

            yield row
            prev_dim_values = dim_values

        # Dump the rest of child rows
        for row in child_row_map.values():
            yield row


def make_binary_merger(placement: BlockPlacement) -> BinaryStreamMerger:
    """
    Factory of BinaryStreamMerger instances.
    """

    placement_type_map: Mapping[QueryBlockPlacementType, Type[BinaryStreamMerger]] = {
        QueryBlockPlacementType.after: AfterBinaryStreamMerger,
        QueryBlockPlacementType.dispersed_after: DispersedAfterBinaryStreamMerger,
    }
    binary_merger_cls = placement_type_map[placement.type]
    binary_merger = binary_merger_cls(placement=placement)
    return binary_merger
