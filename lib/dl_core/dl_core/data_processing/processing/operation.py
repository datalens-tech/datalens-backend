from __future__ import annotations

from typing import (
    AbstractSet,
    ClassVar,
    Collection,
    Hashable,
    Optional,
)

import attr

from dl_core.components.ids import AvatarId
from dl_core.data_processing.stream_base import (
    AbstractStream,
    DataSourceVS,
    DataStreamAsync,
    JointDataSourceVS,
)
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import JoinOnExpressionCtx


# Bases


@attr.s(frozen=True)
class BaseOp:
    """Base class for all operations"""

    supported_source_types: ClassVar[tuple[type[AbstractStream], ...]] = ()
    supported_dest_types: ClassVar[tuple[type[AbstractStream], ...]] = ()

    dest_stream_id: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class SingleSourceOp(BaseOp):
    source_stream_id: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class MultiSourceOp(BaseOp):
    source_stream_ids: AbstractSet[str] = attr.ib(kw_only=True)


# Actual ops


@attr.s(frozen=True)
class UploadOp(SingleSourceOp):
    """Upload data from a stream to a virtual one"""

    supported_source_types = (DataStreamAsync,)
    supported_dest_types = (DataSourceVS,)

    result_id: AvatarId = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class DownloadOp(SingleSourceOp):
    """Download data from a table (virtual stream) to a real data stream"""

    supported_source_types = (DataSourceVS,)
    supported_dest_types = (DataStreamAsync,)

    row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class CalcOp(SingleSourceOp):
    """Apply query to the source"""

    supported_source_types = (DataSourceVS, JointDataSourceVS)
    supported_dest_types = (DataSourceVS,)

    result_id: AvatarId = attr.ib(kw_only=True)
    bi_query: BIQuery = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)
    data_key_data: Hashable = attr.ib(kw_only=True)


# To be implemented:


@attr.s(frozen=True)
class JoinOp(MultiSourceOp):  # TODO: Implement me
    """Join several streams"""

    supported_source_types = (DataSourceVS,)
    supported_dest_types = (JointDataSourceVS,)

    join_on_expressions: Collection[JoinOnExpressionCtx] = attr.ib(kw_only=True)
    root_avatar_id: AvatarId = attr.ib(kw_only=True)
    # Workaround for sourceless subqueries in LODs
    use_empty_source: bool = attr.ib(kw_only=True, default=False)


@attr.s(frozen=True)
class UnionOp(MultiSourceOp):  # TODO: Implement me
    """Concatenate several streams"""

    supported_source_types = (DataSourceVS,)
    supported_dest_types = (DataSourceVS,)


@attr.s(frozen=True)
class MapOp(SingleSourceOp):  # TODO: Implement me
    supported_source_types = (DataStreamAsync,)
    supported_dest_types = (DataStreamAsync,)

    mapper_id: str = attr.ib(kw_only=True)
