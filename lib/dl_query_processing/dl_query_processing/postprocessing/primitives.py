from __future__ import annotations

from typing import (
    Any,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import attr

from dl_constants.types import TBIDataValue
from dl_core.utils import attrs_evolve_to_subclass
import dl_query_processing.exc
from dl_query_processing.execution.primitives import ExecutedQueryMetaInfo
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.legend.field_legend import Legend


PostprocessedValue = Union[TBIDataValue, dict]
PostprocessedRow = Tuple[PostprocessedValue, ...]
PostprocessedData = Tuple[PostprocessedRow, ...]


_META_TV = TypeVar("_META_TV", bound="ExecutedQueryMetaInfo")


@attr.s
class PostprocessedQueryMetaInfo(ExecutedQueryMetaInfo):
    @classmethod
    def from_exec_meta(
        cls: type[_META_TV],
        exec_meta: ExecutedQueryMetaInfo,
    ) -> _META_TV:
        return attrs_evolve_to_subclass(
            cls=cls,
            inst=exec_meta,
        )


@attr.s
class PostprocessedQuery:
    postprocessed_data: PostprocessedData = attr.ib(kw_only=True, default=())
    meta: PostprocessedQueryMetaInfo = attr.ib(kw_only=True, factory=PostprocessedQueryMetaInfo)

    def clone(self, **updates: Any) -> PostprocessedQuery:
        return attr.evolve(self, **updates)


@attr.s(frozen=True)
class PostprocessedQueryBlock(BlockSpec):
    postprocessed_query: PostprocessedQuery = attr.ib(kw_only=True)

    @classmethod
    def from_block_spec(cls, block_spec: BlockSpec, postprocessed_query: PostprocessedQuery) -> PostprocessedQueryBlock:
        return attrs_evolve_to_subclass(
            cls=cls,
            inst=block_spec,
            postprocessed_query=postprocessed_query,
        )


@attr.s(frozen=True)
class PostprocessedQueryUnion:
    blocks: list[PostprocessedQueryBlock] = attr.ib(kw_only=True)
    legend: Legend = attr.ib(kw_only=True)
    offset: Optional[int] = attr.ib(kw_only=True)
    limit: Optional[int] = attr.ib(kw_only=True)

    def clone(self, **updates: Any) -> PostprocessedQueryUnion:
        return attr.evolve(self, **updates)

    @property
    def single_postprocessed_query(self) -> PostprocessedQuery:
        if len(self.blocks) != 1:
            raise dl_query_processing.exc.MultipleBlocksUnsupportedError()
        return self.blocks[0].postprocessed_query
