"""
Helpers for fetching data from datasets
"""

from __future__ import annotations

from typing import (
    Collection,
    Optional,
)

import attr

from dl_constants.enums import (
    DataSourceRole,
    ProcessorType,
    SelectorType,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.ids import AvatarId
from dl_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from dl_core.data_processing.processing.operation import (
    CalcOp,
    DownloadOp,
    JoinOp,
)
from dl_core.data_processing.selectors.base import DataSelectorAsyncBase
from dl_core.data_processing.stream_base import (
    DataSourceVS,
    DataStream,
    DataStreamAsync,
)
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import JoinOnExpressionCtx
from dl_core.services_registry import ServicesRegistry
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.us_manager.us_manager import USManagerBase
from dl_utils.aio import (
    await_sync,
    to_sync_iterable,
)


@attr.s
class DataFetcher:
    _dataset: Dataset = attr.ib(kw_only=True)
    _selector: Optional[DataSelectorAsyncBase] = attr.ib(kw_only=True, default=None)
    _service_registry: Optional[ServicesRegistry] = attr.ib(kw_only=True, default=None)
    _selector_type: SelectorType = attr.ib(kw_only=True, default=SelectorType.CACHED)
    _us_manager: Optional[USManagerBase] = attr.ib(kw_only=True, default=None)  # FIXME: Legacy; remove
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_us_entry_buffer.default
    def _make_us_entry_buffer(self) -> USEntryBuffer:
        # TODO: Remove with self._us_manager
        assert self._us_manager is not None
        return self._us_manager.get_entry_buffer()

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    def __attrs_post_init__(self) -> None:
        if self._selector is None:
            assert self._service_registry is not None
            sel_factory = self._service_registry.get_selector_factory()
            self._selector = sel_factory.get_dataset_selector(
                dataset=self._dataset,
                selector_type=self._selector_type,
                us_entry_buffer=self._us_entry_buffer,
            )

    def _get_avatar_virtual_data_stream(
        self,
        avatar_id: AvatarId,
        stream_id: str,
        role: DataSourceRole,
        from_subquery: bool = False,
        subquery_limit: Optional[int] = None,
    ) -> DataSourceVS:
        alias = avatar_id
        prep_component_manager = DefaultPreparedComponentManager(
            dataset=self._dataset,
            role=role,
            us_entry_buffer=self._us_entry_buffer,
        )
        prep_src_info = prep_component_manager.get_prepared_source(
            avatar_id=avatar_id, alias=alias, from_subquery=from_subquery, subquery_limit=subquery_limit
        )
        return DataSourceVS(
            id=stream_id,
            alias=alias,
            result_id=avatar_id,
            names=prep_src_info.col_names,
            user_types=prep_src_info.user_types,
            prep_src_info=prep_src_info,
        )

    async def get_data_stream_async(
        self,
        *,
        bi_query: BIQuery,
        role: DataSourceRole = DataSourceRole.origin,
        row_count_hard_limit: Optional[int] = None,
        root_avatar_id: Optional[AvatarId] = None,
        required_avatar_ids: Optional[Collection[AvatarId]] = None,
        join_on_expressions: Collection[JoinOnExpressionCtx] = (),
        from_subquery: bool = False,
        subquery_limit: Optional[int] = None,
    ) -> DataStreamAsync:
        if root_avatar_id is None:
            root_avatar_id = self._ds_accessor.get_root_avatar_strict().id
        if required_avatar_ids is None:
            required_avatar_ids = bi_query.get_required_avatar_ids()

        dp_factory = self._service_registry.get_data_processor_factory()  # type: ignore  # TODO: fix
        data_processor = await dp_factory.get_data_processor(
            dataset=self._dataset,
            processor_type=ProcessorType.SOURCE_DB,
            selector_type=self._selector_type,
            role=role,
            us_entry_buffer=self._us_entry_buffer,
        )
        streams = [
            self._get_avatar_virtual_data_stream(
                avatar_id=avatar_id,
                stream_id=f"ava_{avatar_id}",
                role=role,
                from_subquery=from_subquery,
                subquery_limit=subquery_limit,
            )
            for avatar_id in sorted(required_avatar_ids)
        ]
        operations = [
            JoinOp(
                source_stream_ids={f"ava_{avatar_id}" for avatar_id in sorted(required_avatar_ids)},
                dest_stream_id="join_0",
                join_on_expressions=join_on_expressions,
                root_avatar_id=root_avatar_id,
            ),
            CalcOp(
                source_stream_id="join_0",
                dest_stream_id="calc_0",
                result_id="res",
                bi_query=bi_query,
                alias="res",
            ),
            DownloadOp(
                source_stream_id="calc_0",
                dest_stream_id="down_0",
                row_count_hard_limit=row_count_hard_limit,
            ),
        ]
        output_streams = await data_processor.run(
            streams=streams,
            operations=operations,
            output_stream_ids={"down_0"},
        )
        assert len(output_streams) == 1, f"Expected 1 stream, got {len(output_streams)}"
        data_stream = output_streams[0]
        return data_stream

    def get_data_stream(
        self,
        *,
        bi_query: BIQuery,
        role: DataSourceRole = DataSourceRole.origin,
        row_count_hard_limit: Optional[int] = None,
        root_avatar_id: Optional[AvatarId] = None,
        required_avatar_ids: Optional[Collection[AvatarId]] = None,
        join_on_expressions: Collection[JoinOnExpressionCtx] = (),
        from_subquery: bool = False,
        subquery_limit: Optional[int] = None,
    ) -> DataStream:
        async_data_stream = await_sync(
            self.get_data_stream_async(
                role=role,
                bi_query=bi_query,
                row_count_hard_limit=row_count_hard_limit,
                root_avatar_id=root_avatar_id,
                required_avatar_ids=required_avatar_ids,
                join_on_expressions=join_on_expressions,
                from_subquery=from_subquery,
                subquery_limit=subquery_limit,
            )
        )
        return DataStream(
            id=async_data_stream.id,
            names=async_data_stream.names,
            user_types=async_data_stream.user_types,
            meta=async_data_stream.meta,
            data=to_sync_iterable(async_data_stream.data.items),
            data_key=async_data_stream.data_key,
        )
