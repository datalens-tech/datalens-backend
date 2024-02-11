from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
)

import attr


if TYPE_CHECKING:
    from dl_cache_engine.primitives import LocalKeyRepresentation
    from dl_constants.enums import UserDataType
    from dl_constants.types import TBIDataRow
    from dl_core.components.ids import AvatarId
    from dl_core.data_processing.prepared_components.primitives import (
        PreparedMultiFromInfo,
        PreparedSingleFromInfo,
    )
    from dl_core.data_processing.types import TValuesChunkStream
    import dl_core.data_source


_DATA_STREAM_TV = TypeVar("_DATA_STREAM_TV", bound="AbstractStream")


@attr.s
class AbstractStream:
    id: str = attr.ib(kw_only=True)
    names: Sequence[str] = attr.ib(kw_only=True)
    user_types: Sequence[UserDataType] = attr.ib(kw_only=True)
    data_key: Optional[LocalKeyRepresentation] = attr.ib(kw_only=True)
    meta: DataRequestMetaInfo = attr.ib(kw_only=True)

    def clone(self: _DATA_STREAM_TV, **kwargs: Any) -> _DATA_STREAM_TV:
        return attr.evolve(self, **kwargs)


@attr.s
class DataRequestMetaInfo:
    query_id: Optional[str] = attr.ib(default=None, kw_only=True)
    query: Optional[str] = attr.ib(default=None, kw_only=True)
    data_source_list: Collection[dl_core.data_source.DataSource] = attr.ib(default=(), kw_only=True)
    pass_db_query_to_user: bool = attr.ib(default=True, kw_only=True)


@attr.s
class DataStreamBase(AbstractStream):
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
class AsyncVirtualStream(AbstractStream):
    """A representation of data that is being streamed in an external system (database)"""

    _preparation_callback: Optional[Callable[[], Awaitable[None]]] = attr.ib(kw_only=True)

    async def prepare(self) -> None:
        if self._preparation_callback is None:
            return
        await self._preparation_callback()


@attr.s
class DataSourceVS(AsyncVirtualStream):
    """A data source avatar"""

    result_id: AvatarId = attr.ib(kw_only=True)
    alias: str = attr.ib(kw_only=True)
    prep_src_info: PreparedSingleFromInfo = attr.ib(kw_only=True)


@attr.s
class JointDataSourceVS(AsyncVirtualStream):
    """Joint data source info"""

    joint_dsrc_info: PreparedMultiFromInfo = attr.ib(kw_only=True)
