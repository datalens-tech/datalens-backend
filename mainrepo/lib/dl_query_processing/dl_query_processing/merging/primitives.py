from __future__ import annotations

from typing import (
    Any,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
)

import attr

from dl_query_processing.enums import QueryType
from dl_query_processing.legend.field_legend import Legend
from dl_query_processing.postprocessing.primitives import PostprocessedRow


@attr.s(frozen=True)
class MergedQueryBlockMetaInfo:
    block_id: int = attr.ib(kw_only=True)
    query_type: QueryType = attr.ib(kw_only=True)
    debug_query: Optional[str] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class MergedQueryMetaInfo:
    blocks: List[MergedQueryBlockMetaInfo] = attr.ib(kw_only=True)
    offset: Optional[int] = attr.ib(kw_only=True, default=None)
    limit: Optional[int] = attr.ib(kw_only=True, default=None)
    target_connection_ids: set[str] = attr.ib(kw_only=True, factory=set)


class MergedQueryDataRow(NamedTuple):
    """This class should be as simple and as small as possible, so use NamedTuple instead of attrs"""

    data: PostprocessedRow
    legend_item_ids: Sequence[int]


MergedQueryDataRowIterable = Iterable[MergedQueryDataRow]


@attr.s(frozen=True)
class MergedQueryDataStream:
    rows: MergedQueryDataRowIterable = attr.ib(kw_only=True)
    legend: Legend = attr.ib(kw_only=True)
    legend_item_ids: Optional[Sequence[int]] = attr.ib(kw_only=True)  # in case there is only one stream, for v1
    meta: MergedQueryMetaInfo = attr.ib(kw_only=True)

    def clone(self, **updates: Any) -> MergedQueryDataStream:
        return attr.evolve(self, **updates)
