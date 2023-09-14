from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Collection,
    Iterable,
    Optional,
    Sequence,
)

import attr

if TYPE_CHECKING:
    from bi_constants.enums import BIType
    from bi_constants.types import TBIDataRow
    from bi_core.components.ids import AvatarId
    from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
    from bi_core.data_processing.prepared_components.primitives import (
        PreparedMultiFromInfo,
        PreparedSingleFromInfo,
    )
    from bi_core.data_processing.types import TValuesChunkStream
    import bi_core.data_source


@attr.s
class AbstractStream:
    id: str = attr.ib(kw_only=True)
    names: Sequence[str] = attr.ib(kw_only=True)
    user_types: Sequence[BIType] = attr.ib(kw_only=True)


@attr.s
class DataRequestMetaInfo:
    query_id: Optional[str] = attr.ib(default=None, kw_only=True)
    query: Optional[str] = attr.ib(default=None, kw_only=True)
    is_materialized: bool = attr.ib(default=False, kw_only=True)
    data_source_list: Collection[bi_core.data_source.DataSource] = attr.ib(default=(), kw_only=True)
    pass_db_query_to_user: bool = attr.ib(default=True, kw_only=True)


@attr.s
class DataStreamBase(AbstractStream):
    meta: DataRequestMetaInfo = attr.ib(kw_only=True)
    data_key: Optional[LocalKeyRepresentation] = attr.ib(kw_only=True)
    # data: either sync or async, defined in subclasses

    @property
    def debug(self) -> DataRequestMetaInfo:  # TODO: Remove
        return self.meta


@attr.s
class DataStream(DataStreamBase):
    data: Iterable[TBIDataRow] = attr.ib(kw_only=True)


@attr.s
class DataStreamAsync(DataStreamBase):
    data: TValuesChunkStream = attr.ib(kw_only=True)


@attr.s
class VirtualStream(AbstractStream):
    """A representation of data that is being streamed in an external system (database)"""


@attr.s
class DataSourceVS(VirtualStream):
    """A data source avatar"""

    result_id: AvatarId = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)
    prep_src_info: PreparedSingleFromInfo = attr.ib(kw_only=True)


@attr.s
class JointDataSourceVS(VirtualStream):
    """Joint data source info"""

    joint_dsrc_info: PreparedMultiFromInfo = attr.ib(kw_only=True)


@attr.s
class CacheVirtualStream(DataStreamBase):
    """Represents a virtual stream for cache processor. Has no real data"""
