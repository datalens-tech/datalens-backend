from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
    Union,
)

import attr

from bi_constants.enums import BIType
from bi_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from bi_core.data_processing.processing.db_base.exec_adapter_base import ProcessorDbExecAdapterBase
from bi_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from bi_core.query.bi_query import QueryAndResultInfo

if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import Select

    from bi_constants.enums import DataSourceRole
    from bi_core.data_processing.cache.primitives import LocalKeyRepresentation
    from bi_core.data_processing.prepared_components.manager_base import PreparedComponentManagerBase
    from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
    from bi_core.data_processing.selectors.base import DataSelectorAsyncBase
    from bi_core.data_processing.types import TValuesChunkStream
    from bi_core.us_dataset import Dataset
    from bi_core.us_manager.local_cache import USEntryBuffer


@attr.s
class SourceDbExecAdapter(ProcessorDbExecAdapterBase):  # noqa
    _role: DataSourceRole = attr.ib(kw_only=True)
    _dataset: Dataset = attr.ib(kw_only=True)
    _selector: DataSelectorAsyncBase = attr.ib(kw_only=True)
    _prep_component_manager: Optional[PreparedComponentManagerBase] = attr.ib(kw_only=True, default=None)
    _row_count_hard_limit: Optional[int] = attr.ib(kw_only=True, default=None)
    _us_entry_buffer: USEntryBuffer = attr.ib(kw_only=True)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        if self._prep_component_manager is None:
            self._prep_component_manager = DefaultPreparedComponentManager(
                dataset=self._dataset,
                role=self._role,
                us_entry_buffer=self._us_entry_buffer,
            )

    def get_prep_component_manager(self) -> PreparedComponentManagerBase:
        assert self._prep_component_manager is not None
        return self._prep_component_manager

    def _make_query_res_info(
        self,
        query: Union[str, Select],
        user_types: Sequence[BIType],
    ) -> QueryAndResultInfo:
        query_res_info = QueryAndResultInfo(
            query=query,  # type: ignore  # TODO: fix
            user_types=list(user_types),
            # This is basically legacy and will be removed.
            # col_names are not really used anywhere, just passed around a lot.
            # So we generate random ones here
            col_names=[f"col_{i}" for i in range(len(user_types))],
        )
        return query_res_info

    async def _execute_and_fetch(
        self,
        *,
        query: Union[str, Select],
        user_types: Sequence[BIType],
        chunk_size: int,
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
        query_id: str,
    ) -> TValuesChunkStream:
        assert not isinstance(query, str), "String queries are not supported by source DB processor"
        assert joint_dsrc_info is not None, "joint_dsrc_info is required for source DB processor"

        query_res_info = self._make_query_res_info(query=query, user_types=user_types)
        data_stream = await self._selector.get_data_stream(
            query_id=query_id,
            role=self._role,
            joint_dsrc_info=joint_dsrc_info,
            query_res_info=query_res_info,
            row_count_hard_limit=self._row_count_hard_limit,
        )
        return data_stream.data

    def get_data_key(
        self,
        *,
        query_id: str,
        query: Union[str, Select],
        user_types: Sequence[BIType],
        joint_dsrc_info: Optional[PreparedMultiFromInfo] = None,
    ) -> Optional[LocalKeyRepresentation]:
        selector = self._selector
        assert isinstance(selector, DatasetDataSelectorAsyncBase)
        query_res_info = self._make_query_res_info(query=query, user_types=user_types)
        query_execution_ctx = selector.build_query_execution_ctx(
            query_res_info=query_res_info,
            joint_dsrc_info=joint_dsrc_info,  # type: ignore  # TODO: fix
            role=self._role,
            query_id=query_id,
        )
        data_key = selector.get_data_key(query_execution_ctx=query_execution_ctx)
        return data_key
